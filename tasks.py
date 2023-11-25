import asyncio
import traceback
from datetime import datetime as dt, timedelta as td, timezone as tz
from enum import Enum
from statistics import mean
from typing import Union, Any, TypedDict

import loguru
from apscheduler.triggers.date import DateTrigger
from async_lru import alru_cache
from tortoise.expressions import Q, F
from tortoise.functions import Min
from tortoise.query_utils import Prefetch
from web import settings
from web.errors.app import InconsistencyError
from web.kernel.proc.isolate import Isolate
from web.kernel.types import TaskEvent, Environment

from pqueue_app.api.dto.access import EntityId
from pqueue_app.enviroment.infrastructure.database.models import Office, OperatorSession, \
    Ticket, TicketStatus, Issue, IssueCloseCode, TicketCompletion, AttendanceFinishStatus, Attendance, \
    OfficeParamsOptions, WorkplaceSession, OperatorQualityMark, IssueSession, Service, Workplace, PersonalQueue, \
    Operator, OperatorPause
from pqueue_app.enviroment.infrastructure.event import AdminConnectEvent, AdminDisconnectEvent, OfficeInfoEvent, \
    OfficeCurrentTicketsEvent, OfficeServedTicketsEvent, OfficeOperatorsInfoEvent, OfficeServicesEvent, \
    TabChangeEvent, CompetencesUpdatedEvent, OfficeStatisticsUpdatedEvent
from pqueue_app.enviroment.service.details import get_operator_competences
from pqueue_app.utils.utils import get_office_param


class OperatorStatus(Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    WAITING = "waiting"
    SERVES = "serves"
    ON_BREAK = "on_break"


async def closed_tickets(env, office_id: int, reset_at: dt) -> None:
    tickets = await Ticket.filter(office_id=office_id,
                                  created_at__lt=reset_at
                                  ).exclude(status__in=[TicketStatus.COMPLETED,
                                                        TicketStatus.REJECTED,
                                                        TicketStatus.DAILY_PURGE]
                                            ).prefetch_related("issues", "attendances")
    if tickets:
        tickets_list, issues, attendances = [], [], []
        for ticket in tickets:
            ticket.status = TicketStatus.DAILY_PURGE.value
            ticket.completion_status = TicketCompletion.DAILY_PURGE.value

            async for issue in ticket.issues:
                if issue.closed:
                    continue
                issue.closed = True
                issue.close_code = IssueCloseCode.DAILY_PURGE.value
                issues.append(issue)

            async for attendance in ticket.attendances:
                if attendance.finish_status:
                    continue
                attendance.finish_status = AttendanceFinishStatus.DAILY_PURGE.value
                attendances.append(attendance)
            tickets_list.append(ticket)

        await Ticket.bulk_update(tickets_list, fields=["status", "completion_status"])
        if issues:
            await Issue.bulk_update(issues, fields=["closed", "close_code"])
        if attendances:
            await Attendance.bulk_update(attendances, fields=["finish_status"])


async def reset_workplace_sessions(env, office_id: int, reset_at: dt) -> None:
    workplace_sessions = await WorkplaceSession.filter(acquired_at__lt=reset_at,
                                                       released_at=None,
                                                       workplace__office_id=office_id)
    if workplace_sessions:
        await OperatorSession.filter(
            workplace_id__in=[wps.workplace_id for wps in workplace_sessions]).update(
            expire_time=dt.now().astimezone(),
            workplace_id=None
        )
        for wps in workplace_sessions:
            wps.released_at = reset_at
            await wps.save()


async def check_quality_mark(office: Office, task_period: td) -> None:
    start = dt.now().astimezone() - task_period * 2
    end = dt.now().astimezone() - task_period
    attendances = await Attendance.filter(Q(operator__workplacesessions__released_at__isnull=True) |
                                          Q(operator__workplacesessions__released_at__gt=dt.now().astimezone() - task_period * 2),
                                          operator__workplacesessions__acquired_at__lt=F("end_time"),
                                          operator__workplacesessions__workplace__surveypanels__deleted=False,
                                          ticket__office_id=office.id,
                                          end_time__range=(start, end),
                                          operatorqualitymarks=None).select_related("operator")
    if attendances:
        for attendance in attendances:
            loguru.logger.info("Посетитель отказался ставить оценку оператору - "
                               "| Талон id: {ticket_id} | Оператор id: {operator_id} | Имя оператора: {operator_name}",
                               ticket_id=attendance.ticket_id,
                               operator_id=attendance.operator.id, operator_name=attendance.operator.username)


async def check_overrun_waiting_time(office: Office, task_period: float) -> None:
    """Checking for exceeding the permissible average waiting time"""
    services = await get_services()
    query = Ticket.filter(office_id=office.id,
                          status=TicketStatus.WAITING_GLOBAL,
                          issues__closed=False,
                          issues__service__waiting_standard__isnull=False,
                          ).prefetch_related("issues").distinct()
    tickets_first = await query.filter(calls__isnull=True)
    for ticket in tickets_first:
        for issue in ticket.issues:
            service = services[issue.service_id]
            if not service.waiting_standard:
                continue
            if service.waiting_standard < int((dt.now().astimezone() - ticket.created_at).total_seconds(
            )) < service.waiting_standard + task_period:
                loguru.logger.info("Превышено допустимое среднее время ожидания | Талон id: {ticket_id} | "
                                   "Услуга id: {service_id} | Название услуги: {service_name}",
                                   ticket_id=ticket.id, service_id=service.id, service_name=service.name)

    tickets_repeatedly = await query.filter(calls__isnull=False).prefetch_related("issues", "attendances")
    for ticket in tickets_repeatedly:
        for issue in ticket.issues:
            service = services[issue.service_id]
            if issue.closed or not service.waiting_standard:
                continue
            if service.waiting_standard < int((dt.now().astimezone() - max(
                    [attendance.end_time for attendance in ticket.attendances])).total_seconds(
            )) < service.waiting_standard + task_period:
                loguru.logger.info("Превышено допустимое среднее время ожидания | Талон id: {ticket_id} | "
                                   "Услуга id: {service_id} | Название услуги: {service_name}",
                                   ticket_id=ticket.id, service_id=service.id, service_name=service.name)


async def check_overrun_serving_time(office: Office, task_period: float) -> None:
    """Checking for exceeding the permissible average or maximum serving time"""
    services = await get_services()
    issue_sessions = await IssueSession.filter(issue__ticket__office_id=office.id,
                                               end_time__isnull=True).prefetch_related("issue")
    for iss in issue_sessions:
        service = services[iss.issue.service_id]
        iss_period = int(((dt.now().astimezone()) - iss.begin_time).total_seconds())
        if service.avg_serving_time_standard and service.avg_serving_time_standard < iss_period < service.avg_serving_time_standard + task_period:
            loguru.logger.info("Превышено допустимое среднее время обслуживания | Талон id: {ticket_id} | "
                               "Услуга id: {service_id} | Название услуги: {service_name}",
                               ticket_id=iss.issue.ticket_id, service_id=service.id,
                               service_name=service.name)
        if service.max_serving_time_standard and service.max_serving_time_standard < iss_period < service.max_serving_time_standard + task_period:
            loguru.logger.info("Превышено максимальное время обслуживания посетителя | Талон id: {ticket_id} | "
                               "Услуга id: {service_id} | Название услуги: {service_name}",
                               ticket_id=iss.issue.ticket_id, service_id=service.id,
                               service_name=service.name)


async def check_max_office_capacity(env, office_id: EntityId, max_capacity: int) -> None:
    tickets = await Ticket.filter(office_id=office_id,
                                  status__in=[TicketStatus.WAITING_GLOBAL,
                                              TicketStatus.IN_WORK,
                                              TicketStatus.WAITING_PERSONAL])
    if len(tickets) > max_capacity:
        loguru.logger.info("Превышено допустимое количество посетителей в отделении | "
                           "Офис id: {office_id} | Сейчас посетителей: {tickets} | "
                           "Допустимое количество посетителей: {max_capacity}",
                           office_id=office_id, tickets=len(tickets), max_capacity=max_capacity)


async def check_serving_task(env, period: int) -> None:
    TASK_PERIOD = td(seconds=period)
    offices = await get_offices()
    for office in offices.values():
        await check_quality_mark(office, TASK_PERIOD)
        await check_overrun_waiting_time(office, TASK_PERIOD.total_seconds())
        await check_overrun_serving_time(office, TASK_PERIOD.total_seconds())


@alru_cache(maxsize=2, ttl=300)
async def get_services() -> dict[int, Service]:
    return {s.id: s for s in await Service.all()}


@alru_cache(maxsize=2, ttl=300)
async def get_office_services(office_id: int) -> dict[int, Service]:
    return {service.id: service for service in await Service.filter(offices__id=office_id)}


@alru_cache(maxsize=2, ttl=300)
async def get_offices() -> dict[int, Office]:
    return {office.id: office for office in await Office.all()}


@alru_cache(ttl=300)
async def get_workplaces(office_id: int) -> dict[int, Workplace]:
    return {wplc.id: wplc for wplc in await Workplace.filter(office_id=office_id)}


@alru_cache(ttl=120)
async def get_office_authorized_operators(office_id: int) -> dict[int, Operator]:
    return {operator.id: operator for operator in
            await Operator.filter(office_id=office_id, operatorsessions__expire_time__gt=dt.now().astimezone().replace(
                hour=0, minute=0, second=0, microsecond=0
            ))}


@alru_cache(ttl=30)
async def get_workplace_sessions(office_id: int, start_time: dt) -> list[WorkplaceSession]:
    return await WorkplaceSession.filter(Q(released_at__isnull=True) | Q(released_at__gt=start_time),
                                         workplace__office_id=office_id, acquired_at__gt=start_time - td(days=1))


@alru_cache(ttl=10)
async def get_office_wait_tickets(office_id: int) -> list[Ticket]:
    # prefetches need for calc waiting time
    return await Ticket.filter(office_id=office_id,
                               status__in=[TicketStatus.WAITING_GLOBAL]).prefetch_related("issues", Prefetch(
        "attendances", queryset=Attendance.filter(
            finish_status__in=(AttendanceFinishStatus.REGULAR, AttendanceFinishStatus.REDIRECT))))


@alru_cache(ttl=15)
async def get_office_served_tickets_count(office_id: int, start_time: dt) -> int:
    return await Ticket.filter(office_id=office_id,
                               completed_at__gt=start_time,
                               status=TicketStatus.COMPLETED).count()


async def calc_time(tickets: list[Ticket]):
    """
    Calculate the elapsed time for each ticket and return the maximum, minimum, and average elapsed times.
    """
    if not tickets or len(tickets) < 1:
        return None, None, None

    current_time = dt.now().astimezone()
    times = []
    for ticket in tickets:
        if ticket.status == TicketStatus.IN_WORK:
            times.append((current_time - ticket.last_call).total_seconds())
        elif ticket.status in [TicketStatus.WAITING_GLOBAL, TicketStatus.WAITING_PERSONAL]:
            if not ticket.calls:
                times.append((current_time - ticket.created_at).total_seconds())
            else:
                if not ticket.attendances._fetched:
                    await ticket.fetch_related('attendances')
                attendance = max(ticket.attendances, key=lambda x: x.end_time)
                times.append((current_time - attendance.end_time).total_seconds())

    return round(max(times)), round(min(times)), round(mean(times))


ServicesTickets = TypedDict(
    'ServicesTickets', {'service_id': int, 'tickets': set[Ticket]}
)


async def get_tickets(services: dict, office: Office) -> ServicesTickets:
    """
    Get tickets that have unclosed attendance for the given services and office.

    Args:
        services (dict): A dictionary containing service IDs as keys.
        office (Office): An instance of the Office class.

    Returns:
        ServicesTickets: A dictionary containing service IDs as keys and a set of tickets as values.
    """
    issues = await Issue.filter(service_id__in=services, closed=False,
                                ticket__office_id=office.id).select_related("ticket").prefetch_related(
        'ticket__attendances')

    tickets_service = {}

    for service_id in services:
        tickets = {issue.ticket for issue in issues if issue.service_id == service_id}
        tickets_service[service_id] = tickets

    return tickets_service


ServicesOfficeInfo = TypedDict("ServicesOfficeInfo", {"id": int,
                                                      "name": str,
                                                      "waiting_tickets_count": int,
                                                      "max_time_waiting": int | None,
                                                      "min_time_waiting": int | None,
                                                      "avg_time_waiting": int | None,
                                                      "in_work_tickets_count": int})


async def get_services_info_from_office(services: dict[int, Service], tickets: dict[int, set[Ticket]]) -> \
        list[ServicesOfficeInfo]:
    """
    Retrieves a list of ServicesOfficeInfo objects containing information about each service.
    Each dictionary represents a service and includes the following keys:
        - "id" (int): service id,
        - "name" (str): services name,
        - "waiting_tickets_count" (int): amount of waiting tickets count,
        - "max_time_waiting" (int): maximum waiting time,
        - "min_time_waiting" (int): minimum waiting time,
        - "avg_time_waiting" (int): average waiting time,
        - "in_work_tickets_count" (int): amount of in work tickets
    """
    result = []

    for service_id in services:
        waiting_tickets = []
        in_work_tickets = []
        for ticket in tickets[service_id]:
            if ticket.status in [TicketStatus.WAITING_GLOBAL, TicketStatus.WAITING_PERSONAL]:
                waiting_tickets.append(ticket)
            elif ticket.status == TicketStatus.IN_WORK:
                in_work_tickets.append(ticket)

        max_time_waiting, min_time_waiting, avg_time_waiting = await calc_time(waiting_tickets)
        result.append({
            "id": service_id,
            "name": services[service_id].name,
            "waiting_tickets_count": len(waiting_tickets),
            "max_time_waiting": max_time_waiting,
            "min_time_waiting": min_time_waiting,
            "avg_time_waiting": avg_time_waiting,
            "in_work_tickets_count": len(in_work_tickets)
        })
    return result


def get_ticket_wait_datetime(ticket: Ticket) -> dt:
    if not ticket.calls:
        return ticket.created_at
    else:
        return max([attendance.end_time for attendance in ticket.attendances if attendance.end_time is not None])


def get_ticket_wait_time(ticket: Ticket) -> int:
    return int((dt.now().astimezone() - get_ticket_wait_datetime(ticket)).total_seconds())


OfficeInfo = TypedDict("OfficeInfo", {"name": str,
                                      "services": list[ServicesOfficeInfo],
                                      "total_workplaces": list[dict],
                                      "open_workplaces": int,
                                      "avg_of_total_waiting_tickets": int,
                                      "max_time_in_work": int | None,
                                      "min_time_in_work": int | None,
                                      "avg_time_in_work": int | None,
                                      })


async def get_office_info(office_id: int) -> OfficeInfo:
    """
    Get information about an office.

    "name" (str): office name,
    "services:" (list[dict]): list of dictionaries with information about services,
    "total_workplaces" (list[dict]): information about available workplaces,
    "open_workplaces" (int): amount of open workplaces,
    "avg_of_total_waiting_tickets" (int): average number of waiting tickets,
    "max_time_in_work" (int): maximum tickets service time,
    "min_time_in_work" (int): minimum tickets service time,
    "avg_time_in_work" (int): average tickets service time,
    """
    all_offices = await get_offices()
    office = all_offices.get(office_id)
    total_workplaces = await get_workplaces(office_id=office_id)
    open_workplaces = await Workplace.filter(workplacesessions__released_at=None).count()
    services = await get_office_services(office_id=office_id)
    all_tickets = await get_tickets(services, office)
    tickets_in_work = []
    for tickets_in_service in all_tickets.values():
        for ticket in tickets_in_service:
            if ticket.status == TicketStatus.IN_WORK:
                tickets_in_work.append(ticket)
    services_info = await get_services_info_from_office(services, all_tickets)
    max_time_in_work, min_time_in_work, avg_time_in_work = await calc_time(tickets_in_work)
    waiting_tickets_count_list = [serv_dict['waiting_tickets_count'] for serv_dict in services_info]
    result = {
        "name": office.name,
        "services:": services_info,
        "total_workplaces": [await w.values_dict() for w in total_workplaces.values()],
        "open_workplaces": open_workplaces,
        "avg_of_total_waiting_tickets": mean(waiting_tickets_count_list),
        "max_time_in_work": max_time_in_work,
        "min_time_in_work": min_time_in_work,
        "avg_time_in_work": avg_time_in_work,
    }
    return result


OperatorOfficeInfo = TypedDict(
    'OperatorOfficeInfo', {'online_operators_count': int,
                           'gl_waiting_tickets_count': int,
                           'gl_served_tickets_count': int,
                           'max_waiting_timestamp': str | None,
                           'operator_served_tickets_count': int,
                           'operator_work_time_sum': int,
                           'operator_active_session_start_time': str | None
                           }
)


async def get_office_info_for_operator(environment: Environment, office_id: int,
                                       operator_id: int) -> OperatorOfficeInfo:
    """
    1. number of online operators
    2. number of tickets in global waiting (available for operator)
    3. count of served tickets
    4. max waiting time (timestamp of max waiting ticket)
    ----------
    5. operator served tickets count
    6. operator work time (sum of operator workplace sessions time today)
    7. operator last session start time
    """

    all_offices = await get_offices()
    office = all_offices.get(office_id)
    start_time = dt.now(tz=tz(offset=td(seconds=office.time_offset))).replace(hour=0, minute=0, second=0, microsecond=0)
    office_operators = await get_office_authorized_operators(office_id=office_id)
    if not (operator := office_operators.get(operator_id)):
        raise InconsistencyError(message=f"Operator {operator_id} not found")
    online_operators_count = 0
    for operator_id in settings.online_operators:
        if operator_id in office_operators:
            online_operators_count += 1

    waiting_tickets = await get_office_wait_tickets(office_id=office_id)

    served_tickets_count = await get_office_served_tickets_count(office_id=office_id, start_time=start_time)

    max_waiting_timestamp = min(
        [get_ticket_wait_datetime(ticket) for ticket in
         waiting_tickets]).isoformat() if waiting_tickets else None

    operator_fin_attdncs_count = await Attendance.filter(call_time__gt=start_time,
                                                         finish_status__in=(AttendanceFinishStatus.REGULAR,
                                                                            AttendanceFinishStatus.REDIRECT,
                                                                            AttendanceFinishStatus.BREAK),
                                                         begin_time__not_isnull=True,
                                                         operator=operator).count()
    workplace_sessions = await get_workplace_sessions(office_id=office_id, start_time=start_time)
    total_time_in_work = sum([
        (wp_session.released_at - wp_session.acquired_at).total_seconds()
        if wp_session.released_at
        else 0
        for wp_session in workplace_sessions])

    last_session_start_time = None
    for wp_session in workplace_sessions:
        if not wp_session.released_at and wp_session.operator_id == operator.id:
            last_session_start_time = wp_session.acquired_at.isoformat()

    return {"online_operators_count": online_operators_count,
            "gl_waiting_tickets_count": len(waiting_tickets),
            "gl_served_tickets_count": served_tickets_count,
            "max_waiting_timestamp": max_waiting_timestamp,
            "operator_served_tickets_count": operator_fin_attdncs_count,
            "operator_work_time_sum": int(total_time_in_work),
            "operator_active_session_start_time": last_session_start_time,
            }


CurrentTicketInfo = TypedDict(
    'CurrentTicketInfo', {"number": int,
                          "services_name": list[str],
                          "queue_name": list[str],
                          "created_at": str,
                          "status": str,
                          "current_serving_time": int | None,
                          "current_waiting_time": int | None
                          }
)


async def get_office_current_tickets(office_id: int) -> list[CurrentTicketInfo]:
    """
    Retrieves the current tickets for an office.

    Args:
        office_id (int): The ID of the office.

    Returns:
        list[CurrentTicketInfo]: A list of dictionaries containing information about the current tickets.
            Each dictionary represents a ticket and includes the following keys:
            - "number" (int): The ticket number.
            - "services_name" (list[str]): The names of the services associated with the ticket.
            - "queue_name" (list[str]): The names of the queues associated with the ticket.
            - "created_at" (str): The creation timestamp of the ticket in the format "HH:MM:SS".
            - "status" (str): The status of the ticket. May be one of "IN_WORK", "WAITING_GLOBAL", or "WAITING_PERSONAL".
            - "current_serving_time" (int): The current serving time in seconds (only present for tickets in the "IN_WORK" status).
            - "current_waiting_time" (float): The current waiting time in seconds (only present for tickets not in the "IN_WORK" status).
    """
    tickets_list = []
    all_offices = await get_offices()
    office = all_offices.get(office_id)

    tickets = await Ticket.filter(office_id=office_id,
                                  created_at__gt=dt.now().astimezone().replace(hour=0, minute=0, second=0,
                                                                               microsecond=0),
                                  status__in=[TicketStatus.WAITING_GLOBAL, TicketStatus.WAITING_PERSONAL,
                                              TicketStatus.IN_WORK]).prefetch_related("attendances", "issues")
    services = await get_services()

    def get_in_work(ticket: Ticket) -> float:
        attendance = [attendance for attendance in ticket.attendances if attendance.finish_status is None]
        return (dt.now().astimezone() - attendance[0].call_time).total_seconds()

    for ticket in tickets:
        service_names = [services.get(issue.service_id).name for issue in ticket.issues if
                         services.get(issue.service_id)]
        ticket_info = {
            "number": ticket.number,
            "service_names": service_names,
            "queue_name": service_names,
            "created_at": ticket.created_at.strftime("%H:%M:%S"),
            "status": ticket.status.name
        }
        if ticket.status == TicketStatus.IN_WORK:
            ticket_info["current_serving_time"] = int(get_in_work(ticket))
        else:
            ticket_info["current_waiting_time"] = get_ticket_wait_time(ticket)

        tickets_list.append(ticket_info)

    return tickets_list


ServedTicketInfo = TypedDict(
    'ServedTicketInfo', {"id": int,
                         "number": int,
                         "service_names": list[str],
                         "queue_names": list[str],
                         "created_at": str,
                         "total_waiting_duration": int,
                         "total_serving_duration": int,
                         "operator_names": list[str],
                         "status": str,
                         "workplaces": list[str]
                         }
)


async def get_office_served_tickets(office_id: int) -> list[ServedTicketInfo] | list[None]:
    """
    Retrieves a list of served tickets for an office.

    Args:
        office_id (int): The ID of the office.

    Returns:
        list[ServedTicketInfo]: A list of dictionaries containing information about the served tickets.
            Each dictionary represents a ticket and includes the following keys:
            - "id" (int): The ticket ID.
            - "number" (int): The ticket number.
            - "services_name" (list[str]): The names of the services associated with the ticket.
            - "queue_name" (list[str]): The names of the queues associated with the ticket.
            - "created_at" (str): The creation timestamp of the ticket in the format "HH:MM:SS".
            - "total_waiting_duration" (int): The total waiting duration in seconds.
            - "total_solving_duration" (int): The total solving duration in seconds.
            - "operator_names" (list[str]): The names of the operators who served the ticket.
            - "status" (str): The status of the ticket.
            - "workplaces" (list[str]): The name of the workplace where the ticket was served.
    """
    tickets_list = []
    all_offices = await get_offices()
    office = all_offices.get(office_id)
    operators = await get_office_authorized_operators(office_id)

    tickets = await Ticket.filter(office_id=office_id,
                                  completed_at__gt=dt.now(tz=tz(offset=td(seconds=office.time_offset))).replace(
                                      hour=0,
                                      minute=0,
                                      second=0,
                                      microsecond=0),
                                  status=TicketStatus.COMPLETED).select_related("operator").prefetch_related(
        "attendances", "issues", "personalqueues")
    if not tickets:
        return tickets_list
    start_time = min([ticket.created_at for ticket in tickets])
    workplaces = await get_workplaces(office_id=office_id)
    workplace_sessions = await get_workplace_sessions(office_id=office.id, start_time=start_time)
    services = await get_services()
    for ticket in tickets:
        attendances = ticket.attendances
        unique_operators_name = set()
        unique_workplaces = set()
        for attendance in attendances:
            unique_operators_name.add(operators.get(attendance.operator_id).fullname)
            for wplc_session in workplace_sessions:
                if wplc_session.acquired_at < attendance.call_time:
                    if wplc_session.released_at is not None and attendance.call_time > wplc_session.released_at:
                        continue
                    if attendance.operator.id == wplc_session.operator_id:
                        workplace_name = workplaces.get(wplc_session.workplace_id).name
                        unique_workplaces.add(workplace_name)
        service_names = [services.get(issue.service_id).name for issue in ticket.issues if
                         services.get(issue.service_id)]
        ticket_info = {
            "id": ticket.id,
            "number": ticket.number,
            "service_names": service_names,
            "queue_names": service_names,
            "created_at": ticket.created_at.strftime("%H:%M:%S"),
            # "enrolled_at": printing_time.strftime("%H:%M:%S"),
            "total_waiting_duration": ticket.total_waiting_duration,
            "total_solving_duration": ticket.total_solving_duration,
            "operator_names": list(unique_operators_name),
            "status": ticket.status.name,
            "workplaces": list(unique_workplaces)
        }
        tickets_list.append(ticket_info)

    return tickets_list


OperatorInfo = TypedDict(
    'OperatorInfo', {
        "fullname": str,
        "status": str,
        "breeak_info": dict[str, str],
        "workplace": str,
        "served": int,
        "served_time_percent": float
    }
)


async def get_office_operators_info(office_id: int) -> list[OperatorInfo]:
    """
    Retrieves information about the operators in an office.

    Parameters:
        office_id (int): The ID of the office.

    Returns:
        list[OperatorInfo]: A list of OperatorInfo objects containing information about the operators.
        Each dictionary represents an operator and includes the following keys:
            - "fullname" (str): The full name of the operator.
            - "status" (str): The status of the operator. May be "online", "offline", "waiting", "serves", "on_break".
            - "break_info" (dict[str, str]): A dictionary containing information about the operator's break.
            - "workplace" (str): The name of the workplace where the operator is serving.
            - "served" (int): The number of visitors served by this employee for the current day.
            - "served_time_percent" (float): The percentage of time for the current working day during which the employee served visitors.
    """
    result = []
    all_offices = await get_offices()
    office = all_offices.get(office_id)
    start_time = dt.now(tz=tz(offset=td(seconds=office.time_offset))).replace(hour=0, minute=0, second=0, microsecond=0)
    workplaces = await get_workplaces(office_id)
    workplace_sessions = await get_workplace_sessions(office_id=office_id, start_time=start_time)
    operators = await Operator.filter(id__in=[workplace.operator_id for workplace in workplace_sessions],
                                      office_id=office_id).distinct().prefetch_related(
        Prefetch("attendances", queryset=Attendance.filter(call_time__gt=start_time)),
        Prefetch("operatorpauses", queryset=OperatorPause.filter(unpause_at=None)), "operatorpauses__reason")

    for operator in operators:
        operator_info = {
            "fullname": operator.fullname,
            "status": OperatorStatus.ONLINE.name
        }
        if operator.id not in settings.online_operators:
            operator_info["status"] = OperatorStatus.OFFLINE.name
        elif operator.in_work:
            for attendance in operator.attendances:
                if attendance.finish_status is None:
                    if attendance.begin_time is None:
                        operator_info["status"] = OperatorStatus.WAITING.name
                        break
                    operator_info["status"] = OperatorStatus.SERVES.name
                    break
        elif operator.on_pause:
            operator_info["status"] = OperatorStatus.ON_BREAK.name
            operator_pause = next(operator_pause for operator_pause in operator.operatorpauses)
            operator_info["break_info"] = {"reason": operator_pause.reason.text,
                                           "pause_at": operator_pause.pause_at}

        finished_attendances = [attendance for attendance in operator.attendances
                                if attendance.finish_status in (AttendanceFinishStatus.REGULAR,
                                                                AttendanceFinishStatus.REDIRECT,
                                                                AttendanceFinishStatus.BREAK)
                                and attendance.begin_time]
        if not finished_attendances and all([wp_session.released_at for wp_session in workplace_sessions if
                                             wp_session.operator_id == operator.id]):
            operator_info['status'] = OperatorStatus.OFFLINE.name
            result.append(operator_info)
            continue

        earlier_wps = min(
            [wp_session for wp_session in workplace_sessions if wp_session.operator_id == operator.id],
            key=lambda x: x.acquired_at)
        operator_info['workplace'] = workplaces.get(earlier_wps.workplace_id).name
        operator_info['served'] = len(finished_attendances)

        total_served_time = sum([(att.end_time - att.begin_time).total_seconds() for att in finished_attendances])
        total_time_in_work = sum([
            (wp_session.released_at - wp_session.acquired_at).total_seconds()
            if wp_session.released_at and wp_session.operator_id == operator.id
            else (dt.now().astimezone() - wp_session.acquired_at).total_seconds()  # todo fix logic
            for wp_session in workplace_sessions])
        operator_info["served_time_percent"] = round(total_served_time / total_time_in_work * 100, 2)

        result.append(operator_info)

    return result


WaitingTicketInfo = TypedDict(
    "WaitingTicketInfo", {"number": int,
                          "service_names": list[str],
                          "created_at": str,
                          "current_waiting_time": int
                          })


async def get_office_waiting_tickets(office_id: int) -> list[WaitingTicketInfo]:
    """
    Retrieves a list of waiting tickets for an office.

    Args:
        office_id (int): The ID of the office.

    Returns:
        list[WaitingTicketInfo]: A list of waiting ticket information.
        Each dictionary represents a ticket and includes the following keys:
            - "number" (int): The ticket number.
            - "service_names" (list[str]): List of service names associated with the ticket.
            - "created_at" (str): The creation timestamp of the ticket in the format "HH:MM:SS".
            - "current_waiting_time" (int): The current waiting time in seconds.
    """
    tickets_list = []
    all_offices = await get_offices()
    office = all_offices.get(office_id)
    tickets = await Ticket.filter(office_id=office.id,
                                  status__in=[TicketStatus.WAITING_GLOBAL,
                                              TicketStatus.WAITING_PERSONAL]).prefetch_related("issues", Prefetch(
        "attendances", queryset=Attendance.filter(
            finish_status__in=(AttendanceFinishStatus.REGULAR, AttendanceFinishStatus.REDIRECT))))
    services = await get_office_services(office_id=office.id)

    for ticket in tickets:
        service_names = [services[issue.service_id].name for issue in ticket.issues if
                         services.get(issue.service_id)]
        ticket_info = {
            "number": ticket.number,
            "service_names": service_names,
            "created_at": ticket.created_at.strftime("%H:%M:%S"),
        }
        ticket_info["current_waiting_time"] = get_ticket_wait_time(ticket)

        if attendances := ticket.attendances:
            last_attendance = max([attendance.call_time for attendance in attendances])
            ticket_info["created_at"] = last_attendance.call_time

        tickets_list.append(ticket_info)

    return tickets_list


ServicesInfo = TypedDict(
    "ServicesInfo", {"service_name": str,
                     "waiting_standard": int,
                     "open_workplaces_exist": bool,
                     "amount_waiting_tickets": int,
                     "max_waiting_time": int}
)


async def get_office_services_info(office_id: int) -> list[ServicesInfo]:
    """
    Retrieves information about office services.

    Args:
        office_id (int): The ID of the office.

    Returns:
        list[ServicesInfo]: A list of dictionaries containing information about the office services.
        Each dictionary represents a service and includes the following keys:
            - "service_name" (str): The name of the service.
            - "waiting_standard" (int): The waiting time in seconds.
            - "open_workplaces_exist" (bool): Whether there are open workplaces.
            - "amount_waiting_tickets" (int): The number of waiting tickets.
            - "max_waiting_time" (int): The maximum waiting time in seconds.
    """
    services = await get_office_services(office_id=office_id)
    office_workplaces = await get_workplaces(office_id=office_id)

    w_sessions = await WorkplaceSession.filter(workplace_id__in=list(office_workplaces.keys()),
                                               released_at__isnull=True).select_related('operator')
    active_service_workplaces = {}
    for w_session in w_sessions:
        competences = await get_operator_competences(w_session.operator, only_id=True)
        for s_id in competences:
            if s_id in services and s_id not in active_service_workplaces:
                active_service_workplaces[s_id] = True
    tickets = await Ticket.filter(office_id=office_id,
                                  status__in=(TicketStatus.WAITING_GLOBAL, TicketStatus.WAITING_PERSONAL),
                                  issues__service_id__in=[service_id for service_id in services]).prefetch_related(
        'issues', 'attendances')

    services_tickets = {}
    tickets_waiting_time = {}
    result = []

    for ticket in tickets:
        tickets_waiting_time[ticket.id] = get_ticket_wait_time(ticket)
        for issue in ticket.issues:
            if services_tickets.get(issue.service_id):
                services_tickets[issue.service_id].append(ticket)
            else:
                services_tickets[issue.service_id] = [ticket]

    for service in services.values():
        service_info = {
            "service_name": service.name,
            "waiting_standard": service.waiting_standard,
            "open_workplaces_exist": False,
            "amount_waiting_tickets": len(services_tickets.get(service.id, []))
        }

        if tickets_waiting_time:
            if service_tickets := services_tickets.get(service.id):
                service_info["max_waiting_time"] = max([tickets_waiting_time[ticket.id] for ticket in service_tickets])

        if active_service_workplaces.get(service.id):
            service_info["open_workplaces_exist"] = True

        result.append(service_info)
    return result


async def get_office_services_task(office_id):
    result = {
        "services": await get_office_services_info(office_id),
        "tickets": await get_office_waiting_tickets(office_id)}
    return result


async def competence_group_updated_task(env, competence_group_id: EntityId) -> None:
    operators = await Operator.filter(competence_group_id=competence_group_id)
    for operator in operators:
        event = CompetencesUpdatedEvent(office_id=operator.office_id,
                                        operator_id=operator.id)
        await env.channel.produce(event)


async def workplace_competence_updated_task(env, workplace_id: EntityId) -> None:
    operator_session = await OperatorSession.get_or_none(workplace_id=workplace_id).select_related("operator")
    if operator_session:
        event = CompetencesUpdatedEvent(office_id=operator_session.operator.office_id,
                                        operator_id=operator_session.operator.id)
        await env.channel.produce(event)


class PurgeIsolate(Isolate):
    async def work(self, *args, **kwargs) -> None:
        TASK_PERIOD = td(minutes=1)
        first_run = True
        while True:
            offices = await get_offices()
            dt_now = dt.now().astimezone()
            if first_run:
                dt_now -= td(days=1)
            period_time = dt_now + TASK_PERIOD * 2
            for office in offices.values():
                if not (reset_time := await get_office_param(office.id, OfficeParamsOptions.RESET_AT)):
                    continue
                reset_at = dt_now.replace(hour=reset_time.hour,
                                          minute=reset_time.minute,
                                          second=reset_time.second,
                                          tzinfo=tz(offset=td(seconds=office.time_offset)))
                if (period_time > reset_at > dt_now) or first_run:
                    task_kwargs = dict(
                        args=(),
                        trigger=DateTrigger(run_date=reset_at),
                        kwargs=dict(office_id=office.id, reset_at=reset_at))
                    if first_run:
                        task_kwargs.pop('trigger')
                    await self.channel.produce(TaskEvent(closed_tickets, **task_kwargs))
                    await self.channel.produce(TaskEvent(reset_workplace_sessions, **task_kwargs))
            if first_run: first_run = False
            await asyncio.sleep(TASK_PERIOD.total_seconds())


class MonitoringIsolate(Isolate):

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.active_admins: dict[int, list[int]] = dict()
        self.active_offices: dict[int, dict[str, list[int]]] = {}
        self.monitoring_tabs = {"office_monitoring": {"func": get_office_info,
                                                      "event": OfficeInfoEvent},
                                "current_tickets_monitoring": {"func": get_office_current_tickets,
                                                               "event": OfficeCurrentTicketsEvent},
                                "served_tickets_monitoring": {"func": get_office_served_tickets,
                                                              "event": OfficeServedTicketsEvent},
                                "operators_monitoring": {"func": get_office_operators_info,
                                                         "event": OfficeOperatorsInfoEvent},
                                "services_monitoring": {"func": get_office_services_task,
                                                        "event": OfficeServicesEvent},
                                }

    async def __delete_admin_id_from_active_offices(self, office_id: int, admin_id: int) -> None:
        if office_id in self.active_offices:
            for func_name, admins_id in self.active_offices[office_id].items():
                if admin_id in admins_id:
                    admins_id.remove(admin_id)
                    if not admins_id:
                        self.active_offices[office_id].pop(func_name)
                    break
            if not self.active_offices[office_id]:
                self.active_offices.pop(office_id)

    async def __add_admin_id_in_active_offices(self, office_id: int, admin_id: int, tab_name: str) -> None:
        if not self.active_offices.get(office_id):
            self.active_offices[office_id] = {tab_name: [admin_id]}
        elif not self.active_offices[office_id].get(tab_name):
            self.active_offices[office_id][tab_name] = [admin_id]
        else:
            self.active_offices[office_id][tab_name].append(admin_id)

    async def add_active_admin(self, message: AdminConnectEvent) -> None:
        if not self.active_admins.get(message.office_id):
            self.active_admins[message.office_id] = [message.admin_id]
        else:
            if message.admin_id not in self.active_admins[message.office_id]:
                self.active_admins[message.office_id].append(message.admin_id)
            else:
                await self.__delete_admin_id_from_active_offices(message.office_id, message.admin_id)

        await self.__add_admin_id_in_active_offices(message.office_id, message.admin_id, message.default_tab_name)

    async def remove_active_admin(self, message: AdminDisconnectEvent) -> None:
        await self.__delete_admin_id_from_active_offices(admin_id=message.admin_id, office_id=message.office_id)
        if self.active_admins.get(message.office_id):
            if message.admin_id in self.active_admins[message.office_id]:
                self.active_admins[message.office_id].remove(message.admin_id)

                if not self.active_admins.get(message.office_id):
                    self.active_admins.pop(message.office_id)

    async def change_tab(self, message: TabChangeEvent) -> None:
        admin_id = message.admin_id
        tab_name = message.tab_name
        office_id = message.office_id

        await self.__delete_admin_id_from_active_offices(office_id=office_id, admin_id=admin_id)
        await self.__add_admin_id_in_active_offices(office_id=office_id, admin_id=admin_id, tab_name=tab_name)

    async def work(self, *args, **kwargs) -> None:
        self.channel.add_event_listener(AdminConnectEvent, callback=self.add_active_admin)
        self.channel.add_event_listener(AdminDisconnectEvent, callback=self.remove_active_admin)
        self.channel.add_event_listener(TabChangeEvent, callback=self.change_tab)

        # {operator_id: {"last_res": OperatorOfficeInfo, "last_res_ts": datetime, "operator": Operator}}
        active_operators = {}

        while True:
            await asyncio.sleep(1)
            try:
                if not self.active_offices and not settings.online_operators:
                    continue
                if self.active_offices:
                    for office_id, functions in self.active_offices.items():
                        for func_name in functions:
                            func = self.monitoring_tabs[func_name]['func']
                            event = self.monitoring_tabs[func_name]['event']
                            result = await func(office_id)
                            if self.active_offices.get(office_id) and self.active_offices[office_id].get(func_name):
                                await self.channel.produce(event(office_id=office_id, data=result,
                                                                 users_id=self.active_offices[office_id][func_name]))

                if settings.online_operators:
                    for operator_id in settings.online_operators:
                        if operator_id not in active_operators:
                            operator = await Operator.get_or_none(id=operator_id)
                            if not operator:
                                continue
                            active_operators[operator_id] = {'last_res': None, 'last_res_ts': dt.now(),
                                                             'operator': operator}
                        if dt.now() - active_operators[operator_id]['last_res_ts'] > td(seconds=5):
                            active_operators[operator_id]['last_res_ts'] = dt.now()
                            res = await get_office_info_for_operator(None,
                                                                     operator_id=operator_id,
                                                                     office_id=active_operators[operator_id][
                                                                         'operator'].office_id)
                            if active_operators[operator_id]['last_res'] and all(
                                    v == res[k] for k, v in active_operators[operator_id]['last_res'].items()):
                                continue
                            active_operators[operator_id]['last_res'] = res
                            await self.channel.produce(
                                OfficeStatisticsUpdatedEvent(office_id=operator.office_id, statistics=res))

            except Exception as ex:
                loguru.logger.error(ex)
                # traceback.print_exception(ex)
