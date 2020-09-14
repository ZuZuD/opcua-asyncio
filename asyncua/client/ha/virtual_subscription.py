#!/usr/bin/env python3

import logging
import asyncio

from asyncua import ua, Client, Node
from typing import Dict, List, Iterable, Set, Union, Optional, Tuple, TypeVar, Type
from asyncua.common.subscription import Subscription
from collections import defaultdict

_logger = logging.getLogger(__name__)

SubHandler = TypeVar('T')
DataChange = Dict[Node, Tuple[int, SubHandler]]
SubRetry = Tuple[int, SubHandler]


class VirtualSubscription:
    """
    Manages the subscription for multiples clients.

    Methods should be called without a client, the 
    client is only used for recovery/failover scenarios.
    """
    def __init__(self, ha_client: "HaClient") -> None:
        self.ha_client = ha_client
        self.lock = asyncio.Lock()
        # current data model
        self.subscription: Dict[Client, Subscription] = {}
        self.node_to_handle: Dict[Client, Dict[Node, Int]] = defaultdict(dict)
        self.datachange: Dict[Client, DataChange] = defaultdict(dict)
        # failed actions queues
        self.create_sub_retry: Dict[Client, SubRetry] = {}
        self.unsubscribe_retry: Dict[Client, Set[Node]] = defaultdict(set)
        self.datachange_retry: Dict[Client, DataChange] = defaultdict(dict)
        self.set_monitoring_retry: Dict[Client, ua.MonitoringMode] = {} 
        self.set_publishing_retry: Dict[Client, bool] = {}

    async def create_subscription(self, period, handler, publishing=True, clients=None):
        async with self.lock:
            clients = clients or self.ha_client.get_active_clients()
            coros = [self._create_subscription(c, period, handler, publishing) for c in clients]
            await asyncio.gather(*coros)

    async def _create_subscription(self, client: Client, period, handler, publishing=True, clients=None):
        try:
            sub = await client.create_subscription(period, handler, publishing=True)
            self.subscription[client] = sub
        except Exception:
            # override any pending create_subscription
            self.create_sub_retry[client] = (period, handler)
            self.set_publishing_retry[client] = publishing
            _logger.exception(f"Failed to create subscription for {client}")
            
    async def subscribe_data_change(self,
                                    nodes: Union[Node, Iterable[Node]],
                                    attr=ua.AttributeIds.Value,
                                    queuesize=0,
                                    monitoring=ua.MonitoringMode.Reporting,
                                    clients=None) -> Union[int, List[Union[int, ua.StatusCode]]]:
        async with self.lock:
            _logger.warning(f"In subscribe datachange for nodes: {nodes}")
            clients = clients or self.ha_client.get_active_clients()
            coros = [self._subscribe_data_change(c, nodes, attr, queuesize, monitoring) for c in clients]
            await asyncio.gather(*coros)
            
    async def _subscribe_data_change(self,
                                    client,
                                    nodes: Union[Node, Iterable[Node]],
                                    attr=ua.AttributeIds.Value,
                                    queuesize=0,
                                    monitoring=ua.MonitoringMode.Reporting) -> Union[int, List[Union[int, ua.StatusCode]]]:

        if isinstance(nodes, Node):
            nodes = [nodes]
        try:
            sub = self.subscription[client]
            _logger.warning(f"{sub} in subscribe datachange for nodes {nodes}")
            mids = await sub.subscribe_data_change(nodes, attr, queuesize, monitoring=monitoring)
            # populate the dictionnary of nodes to handles
            for node, mid in zip(nodes, mids):
                self.node_to_handle[client][node] = mid
                self.datachange[client][node] = (attr, queuesize)
            # remove any similar subscription in the queue
                if self.datachange_retry[client].get(node):
                    self.datachange_retry[client].pop(node)
        except Exception:
            for node in nodes:
                self.datachange_retry[client][node] = (attr, queuesize)
            _logger.exception(f"Failed to add datachange for {client}")

    async def unsubscribe(self, nodes: Set[Node], clients=None) -> None:
        async with self.lock:
            clients = clients or self.ha_client.get_active_clients()
            coros = [self._unsubscribe(c, nodes) for c in clients]
            await asyncio.gather(*coros)

    async def _unsubscribe(self, client: Client, nodes: Set[Node]) -> None:
        mids = []
        if client in self.subscription:
            sub = self.subscription[client]
            for node in nodes:
                mid = self.node_to_handle[client].get(node)
                if mid:
                    mids.append(mid)
            try:
                await sub.unsubscribe(mids)
            except Exception:
                self.unsubscribe_retry[client] |= set(nodes)
                return

        # evict the nodes if unsub is successful or if there's no subscription
        for node in nodes:
            if self.node_to_handle[client].get(node):
                self.node_to_handle[client].pop(node)
            if self.datachange_retry[client].get(node):
                self.datachange_retry[client].pop(node)

    async def resubscribe(self, monitoring=ua.MonitoringMode.Reporting, publishing=True, clients=None) -> None:
        async with self.lock:
            clients = clients or self.ha_client.get_active_clients()
            coros = [self._resubscribe(c, monitoring, publishing) for c in clients]
            await asyncio.gather(*coros)

    async def _resubscribe(self, client: Client, monitoring=ua.MonitoringMode.Reporting, publishing=True) -> None:
        # subscriptions and datachanges params are the same for 
        # all the clients so just pick the first available
        sub = self.subscription.get(client)
        sub_retry = self.create_sub_retry.get(client)
        # we can't resub if there was no previous subscription
        if not sub and not sub_retry:
            return
        if sub:
            handler = sub._handler
            period = sub.parameters.RequestedPublishingInterval
            try:
                await sub.delete()
            except Exception:
                pass
            # this sub might be expired on the server side
            self.subscription.pop(client)
        elif sub_retry:
            period = sub_retry[0]
            handler = sub_retry[1]
            self.create_sub_retry.pop(client)
        
        self._reconciliate_before_resub(client)
        datachanges = set()
        for node, datachange in self.datachange.get(client, {}).items(): 
            datachanges.add((node, *datachange))
        for node, datachange in self.datachange_retry.get(client, {}).items():
            datachanges.add((node, *datachange))

        _logger.debug(f"Resubscribing datachange for {client}: {datachanges}")
        await self._create_subscription(client, period, handler, publishing)
        coros = [self._subscribe_data_change(client, *data, monitoring=monitoring) for data in datachanges]
        await asyncio.gather(*coros)

    def _reconciliate_before_resub(self, client: Client) -> None:
        """
        Not AsyncIO safe, should be called from a locked method
        """
        # reconciliation unsubscribe queue and datachange
        remove_from_unsub = set()
        for node in self.unsubscribe_retry.get(client, set()):
            if self.datachange_retry[client].get(node):
                self.datachange_retry[client].pop(node)
            if self.datachange[client].get(node):
                self.datachange[client].pop(node)
        self.unsubscribe_retry[client] -= remove_from_unsub
                
        # resub comes with a new monitoring policy
        if client in self.set_publishing_retry:
            self.set_publishing_retry.pop(client)
            _logger.debug(f"set_publishing_retry cleared for {client}")

        # resub comes with a new publishing policy
        if client in self.set_monitoring_retry:
            self.set_monitoring_retry.pop(client)
            _logger.debug(f"set_monitoring_retry cleared for {client}")

        if client in self.datachange_retry:
            self.datachange_retry.pop(client)
            _logger.debug(f"datachange_retry cleared for {client}")

        # new subscription comes with new datachange handle
        if client in self.node_to_handle:
            self.node_to_handle.pop(client)
            _logger.debug(f"node_to_handle cleared for {client}")
            

    async def replay_failed_events(self, clients=None) -> None:
        async with self.lock:
            clients = clients or self.ha_client.get_active_clients()
            coros = [self._replay_failed_events(c) for c in clients]
            await asyncio.gather(*coros)

    async def _replay_failed_events(self, client: Client) -> None:
        """
        Replay all previously failed events
        """
        if client in self.create_sub_retry:
            # always retry subscription with publishing disabled
            create_params = self.create_sub_retry[client]
            _logger.info(f"in sub retry: {create_params}")
            await self._create_subscription(client, *create_params, publishing=False, clients=[client]) 

        # reconciliation unsub queue and datachange queue.
        remove_from_unsub = set()
        for node in self.unsubscribe_retry.get(client, set()):
            if node in self.datachange_retry[client]:
                self.datachange_retry[client].pop(node)
                remove_from_unsub.add(node)
        self.unsubscribe_retry[client] -= remove_from_unsub

        if self.unsubscribe_retry.get(client):
            _logger.info("in unsub retry")
            await self._unsubscribe(client, self.unsubscribe_retry[client])

        for node, datachange in self.datachange_retry.get(client, {}).items():
            _logger.info("in datachange retry")
            # always subscribe datachange retry event with monitoring disabled since it's replayed below.
            await self._subscribe_data_change(client, node, *datachange, monitoring=ua.MonitoringMode.Disabled)

        if client in self.set_monitoring_retry:
            _logger.info("in monitoring retry")
            mode_params = self.set_monitoring_retry[client]
            await self._set_monitoring_mode(client, mode_params)

        if client in self.set_publishing_retry:
            _logger.info("in publishing retry")
            publish_params = self.set_publishing_retry[client]
            await self._set_publishing_mode(client, publish_params)


    async def set_monitoring_mode(self, monitoring: ua.MonitoringMode, clients=None) -> None:
        async with self.lock:
            clients = clients or self.ha_client.get_active_clients()
            coros = [self._set_monitoring_mode(c, monitoring) for c in clients]
            await asyncio.gather(*coros)
    
    async def _set_monitoring_mode(self, client: Client, monitoring: ua.MonitoringMode) -> None:
        sub = self.subscription.get(client)
        if sub:
            try:
                await sub.set_monitoring_mode(monitoring)
                if client in self.set_monitoring_retry:
                    self.set_monitoring_retry.pop(client)
            except Exception:
                _logger.exception(f"Failed to set monitoring mode for {client}")
                self.set_monitoring_retry[client] = monitoring

    async def set_publishing_mode(self, publishing: bool, clients=None) -> None:
        async with self.lock:
            clients = clients or self.ha_client.get_active_clients()
            coros = [self._set_publishing_mode(c, publishing) for c in clients]
            await asyncio.gather(*coros)
            
    async def _set_publishing_mode(self, client: Client, publishing: bool) -> None:
        sub = self.subscription.get(client)
        if sub:
            try:
                await sub.set_publishing_mode(publishing)
                if client in self.set_publishing_retry:
                    self.set_publishing_retry.pop(client)
            except Exception:
                _logger.exception(f"Failed to set publishing mode for {client}")
                self.set_publishing_retry[client] = publishing

    async def delete(self) -> None:
        # The only thing we care before removing a VirtualSubscription
        # is closing the underlying subscriptions
        async with self.lock:
            coros = [sub.delete() for client, sub in self.subscription.items()]
            await asyncio.gather(*coros, return_exceptions=True)
