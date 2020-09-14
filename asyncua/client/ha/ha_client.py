#!/usr/bin/env python3
import logging
import asyncio

from asyncua import ua, Client, Node
from enum import IntEnum
from dataclasses import dataclass, field
from typing import Dict, List, Iterable, Set, Union, Optional, Tuple, TypeVar, Type
from asyncua.ua.uaerrors import BadSessionClosed, BadSessionNotActivated
from concurrent.futures import CancelledError, TimeoutError
from asyncua.client.ha.virtual_subscription import VirtualSubscription, SubHandler

_logger = logging.getLogger(__name__)

class HaMode(IntEnum):
    # one server active at a time
    COLD = 0
    WARM = 1
    HOT = 2
#    HOT_MIRRORED = 3


class ConnectionStates(IntEnum):
    """
    OPC UA Part 4 - Services Release
    Section 6.6.2.4.2 ServiceLevel
    """
    IN_MAINTENANCE = 0
    NO_DATA = 1
    DEGRADED = 2
    HEALTHY = 200


@dataclass
class ServerInfo:
    url: str = None
    status: ConnectionStates = ConnectionStates(1)

class HaClient:
    """
    The HaClient is responsible for managing non-transparent server redundancy.
    The two servers should have:
        - Identical NodeIds
        - Identical browse path and AddressSpace structure
        - Identical Service Level logic
        - However nodes in the server local namespace can differ
        - NTP synchronization
    It starts the OPC-UA clients and connect to the server that
    fits in the HaMode selected.
    """

    def __init__(
        self,
        urls: List[str],
        ha_mode: HaMode, 
        # in seconds
        keepalive_timer: int = 15,
        manager_timer: int = 15,
        loop=None
    ) -> None:
        self.clients: Dict[Client, ClientStatus] = {}
        self.vsubscriptions: Set["VirtualSubscription"] = set()
        self._vsub_lock: asyncio.Lock = asyncio.Lock()
        self._ha_mode: HaMode = ha_mode
        self._keepalive_timer: int = keepalive_timer
        self._manager_timer: int = manager_timer
        self._keepalive_task: Dict["Keepalive", asyncio.Task] = {}
        self._manager_task: Dict["HaManager", asyncio.Task] = {}
        self._session_timeout: int = 30000
        self._request_timeout: int = 5 
        #self._session_timeout = Client(url="url").session_timeout

        self.loop = loop or asyncio.get_event_loop()
        self.urls = urls


        self.active_client: Optional[Client] = None

        for url in urls:
            c = Client(url, timeout= self._request_timeout, loop=self.loop)
            self.clients[c] = ServerInfo(url)

        # TODO
        # Check if transparent redundancy support exist for the server (nodeid=2035)
        # and prevent against using HaClient

    async def start(self) -> None:
        for client, server in self.clients.items():
            keepalive = KeepAlive(client, server, self._keepalive_timer)
            self._keepalive_task[keepalive] = self.loop.create_task(keepalive.run())
        self._manager = HaManager(self, self._manager_timer) 
        self_manager_task = self.loop.create_task(self._manager.run())
        #await self._manager.run()


    async def stop(self):
        for keepalive, task in self._keepalive_task.items():
            await keepalive.stop()
        for manager, task in self._manager_task.items():
            await manager.stop()
        for client in self.clients:
            try:
                await client.disconnect()
            except Exception:
                pass
        await self.delete_subscriptions(self.vsubscriptions)

    @property
    def ha_mode(self) -> str:
        return self._ha_mode.name.lower()

    async def create_subscription(self, period: int, handler: SubHandler) -> "VirtualSubscription":
        async with self._vsub_lock:
            vs = VirtualSubscription(self)
            await vs.create_subscription(period, handler)
            self.vsubscriptions.add(vs)
            return vs

    async def delete_subscriptions(self, subs: Iterable["VirtualSubscription"]) -> None:
        async with self._vsub_lock:
            coros = [sub.delete() for sub in subs]
            await asyncio.gather(*coros)

    async def resubscribe(self, monitoring=ua.MonitoringMode.Reporting, publishing=True, clients=None) -> None:
        async with self._vsub_lock:
            coros = [vsub.resubscribe(monitoring, publishing, clients=clients) for vsub in self.vsubscriptions]
            await asyncio.gather(*coros)
                
    async def set_monitoring_mode(self, monitoring: ua.MonitoringMode, clients=None) -> None:
        async with self._vsub_lock:
            coros = [vsub.set_monitoring_mode(monitoring, clients=clients) for vsub in self.vsubscriptions]
            await asyncio.gather(*coros)

    async def set_publishing_mode(self, publishing: bool, clients=None) -> None:
        async with self._vsub_lock:
            coros = [vsub.set_publishing_mode(publishing, clients=clients) for vsub in self.vsubscriptions]
            await asyncio.gather(*coros)

    async def replay_failed_events(self, clients: Optional[Iterable[Client]]=None) -> None:
        # Retry any pending subscription operation
        async with self._vsub_lock:
            coros = [vsub.replay_failed_events(clients=clients) for vsub in self.vsubscriptions]
            await asyncio.gather(*coros)

    def get_client_cold_mode(self) -> Client:
        return self.active_client

    def get_client_warm_mode(self) -> List[Client]:
        return list(self.clients)

    def get_active_clients(self) -> List[Client]:
        ha_mode = self.ha_mode
        func = f"get_client_{ha_mode}_mode"
        get_clients = getattr(self, func)
        active_clients = get_clients()
        if not isinstance(active_clients, list):
            active_clients = [active_clients]
        return active_clients

    @property
    def session_timeout(self) -> int:
        return self._session_timeout

    @session_timeout.setter
    def session_timeout(self, value: int) -> None:
        self._session_timeout = value
        for client in self.clients:
            client.session_timeout = value

class KeepAlive:
    """ 
    Ping the server status regularly to check its health
    """
    
    def __init__(self, client, server, timer) -> None:
        self.client = client
        self.server = server
        self.timer = timer
        self.stop_event = asyncio.Event()
        self.is_running = False

    async def stop(self):
        self.stop_event.set()
        
    async def run(self) -> None:
        status_node = self.client.nodes.server_state
        slevel_node = self.client.nodes.service_level
        server_info = self.server
        client = self.client
        # wait for HaManager to connect clients
        await asyncio.sleep(3)
        self.is_running = True
        _logger.info(f"Starting keepalive loop for {server_info.url} checking every {self.timer}sec")
        while self.is_running:
            try:
                status, slevel = await client.read_values([status_node, slevel_node])
                if status != ua.ServerState.Running:
                    _logger.info("ServerState is not running")
                    server_info.status = ConnectionStates.NO_DATA
                else:
                    server_info.status = slevel
            except BadSessionNotActivated:
                _logger.warning("Session is not yet activated.")
                server_info.status = ConnectionStates.NO_DATA
            except BadSessionClosed:
                _logger.warning("Session is closed.")
                server_info.status = ConnectionStates.NO_DATA
            except (TimeoutError, CancelledError):
                _logger.warning("Timeout when fetching state")
                server_info.status = ConnectionStates.NO_DATA
            except Exception:
                _logger.exception(
                    "Unknown exception during keepalive liveness check"
                )
                server_info.status = ConnectionStates.NO_DATA

            _logger.info(f"ServiceLevel for {server_info.url}: {server_info.status}")
            if await event_wait(self.stop_event, self.timer):
                self.is_running = False
                break

    
class HaManager:
    """
    The manager handles individual client connections
    according to its HaMode
    """
    def __init__(self, ha_client: HaClient, timer: Optional[int] = None):
         
        self.ha_client = ha_client
        self.timer = self.set_loop_timer(timer)
        self.stop_event = asyncio.Event()
        self.is_running = False

    def set_loop_timer(self, timer: Optional[int]):
        return timer if timer else int(self.ha_client.session_timeout / 1000)
        
    async def run(self):
        ha_mode = self.ha_client.ha_mode
        update_func = f"update_state_{ha_mode}"
        update_state = getattr(self, update_func)
        reco_func = f"reconnect_{ha_mode}"
        reconnect = getattr(self, reco_func)
        self.is_running = True

        _logger.info(f"Starting HaManager loop checking every {self.timer}sec")
        while self.is_running:

            # failover if needed
            await update_state()

            # reconnect if needed
            await reconnect()
            
            if await event_wait(self.stop_event, self.timer):
                self.is_running = False
                break

    async def stop(self):
        self.stop_event.set()
        

    def group_clients_by_health(self) -> Tuple[List[Client], List[Client]]:
        clients = self.ha_client.clients
        healthy = []
        unhealthy = []
        for client, server in clients.items():
            if server.status > ConnectionStates.DEGRADED:
                healthy.append(client) 
            else:
                unhealthy.append(client)
        return healthy, unhealthy

    def get_serving_client(self, clients: List[Client]) -> Optional[Client]:
        # The service level reference is taken from the active_client
        # Thus we prevent failing over when mutliple clients 
        # return the same service_level
        serving_client = self.ha_client.active_client
        if serving_client:
            max_slevel = self.ha_client.clients[serving_client].status
        else:
            max_slevel = ConnectionStates.NO_DATA

        for c in clients:
            c_slevel = self.ha_client.clients[c].status
            if c_slevel > max_slevel:
                serving_client = c
                max_slevel = c_slevel
        return serving_client


    async def update_state_cold(self):
        # 6.6.2.4.5.2 Cold
        # Only connect to the active_client, failover is managed by 
        # promoting another client of the pool to active_client

        active_client = self.ha_client.active_client
        healthy, unhealthy = self.group_clients_by_health()
        _logger.info(f"Clients healthy: {healthy}, Clients unhealthy: {unhealthy}")
        if healthy and not active_client:
            self.ha_client.active_client = healthy[0]
        # no active client specified -> default to the first client found
        elif unhealthy and not active_client:
            self.ha_client.active_client = unhealthy[0]
        elif active_client not in healthy:
            _logger.warning(f"cold active client {active_client} isn't healthy")

    async def update_state_warm(self):
        # 6.6.2.4.5.3 Warm
        # Enable the active client similarly to the cold mode.
        # Secondary clients create the MonitoredItems,
        # but disable sampling and publishing.

        coros = [] 
        active_client = self.ha_client.active_client
        clients = list(self.ha_client.clients)
        primary_client = self.get_serving_client(clients)
        if primary_client and primary_client != active_client:
            # failover or first connect
            # try to flip over the monitoring mode
            _logger.info(f"Failing over active client from {active_client} to {primary_client}")
            coros.append(self.ha_client.set_monitoring_mode(ua.MonitoringMode.Reporting, clients=[primary_client]))
            coros.append(self.ha_client.set_publishing_mode(True, clients=[primary_client]))
            self.ha_client.active_client = primary_client

            others = set(clients) - set([primary_client])
            coros.append(self.ha_client.set_monitoring_mode(ua.MonitoringMode.Disabled, clients=others))
            coros.append(self.ha_client.set_publishing_mode(False, clients=others))

            await asyncio.gather(*coros, return_exceptions=True)
        
        healthy, unhealthy = self.group_clients_by_health()
        await self.ha_client.replay_failed_events(healthy)

    async def reconnect_warm(self):
        """
        Reconnect disconnected clients
        """
        active_client = self.ha_client.active_client
        #healthy, unhealthy = self.group_clients_by_health()
        connects = []
        resubs = []
        clients = self.ha_client.get_active_clients()
        for client in clients:
            if (
                not client.uaclient.protocol or
                client.uaclient.protocol
                and client.uaclient.protocol.state == "closed" 
            ):
                if client == active_client:
                    publishing = True
                    monitoring=ua.MonitoringMode.Reporting
                else:
                    publishing = False
                    monitoring=ua.MonitoringMode.Disabled
                connects.append(client.reconnect())
                _logger.info(f"Resubscribing {client} with publishing: {publishing} and monitoring: {monitoring.name}")
                resubs.append(self.ha_client.resubscribe(
                    monitoring=monitoring,
                    publishing=publishing,
                    clients=[client]
                ))
        results = await asyncio.gather(*connects, return_exceptions=True)
        for enum, result in enumerate(results):
            if isinstance(result, Exception):
                _logger.exception(f"Error when reconnecting {clients[enum]}: {repr(result)}")

        results = await asyncio.gather(*resubs, return_exceptions=True)
        for enum, result in enumerate(results):
            if isinstance(result, Exception):
                _logger.exception(f"Error when resubscribing {clients[enum]}: {repr(result)}")
            
    async def reconnect_cold(self):
        # in cold mode we only want to keep a connection to the primary
        active_client = self.ha_client.active_client
        healthy, unhealthy = self.group_clients_by_health()
        for client in healthy:
            # close connections that arn't the active_client
            if (
                client != active_client 
                and client.uaclient.protocol
                and client.uaclient.protocol.state == "open"
            ):
                try:
                    _logger.info(f"Disconnecting secondary cold client: {client}")
                    await client.disconnect()
                except Exception:
                    pass

        if active_client not in healthy:
            try:
                _logger.info("Trying to reconnect unhealthy active client")
                await active_client.reconnect()
                await self.ha_client.resubscribe(clients=[active_client])
                # validate the connection?
            except Exception:
                _logger.exception("Exception raised while connecting to active client")


async def event_wait(evt, timeout):
    try:
        await asyncio.wait_for(evt.wait(), timeout)
    except asyncio.TimeoutError:
        pass
    return evt.is_set()
