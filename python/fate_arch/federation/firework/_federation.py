########################################################
# Copyright 2019-2021 program was created VMware, Inc. #
# SPDX-License-Identifier: Apache-2.0                  #
########################################################

import io
import json
import sys
import time
import typing
from pickle import dumps as p_dumps, loads as p_loads

import grpc
from fate_arch.protobuf.python.firework_transfer_pb2_grpc import FireworkQueueServiceStub
from pyspark import SparkContext

from fate_arch.abc import FederationABC, GarbageCollectionABC
from fate_arch.common import Party
from fate_arch.common.log import getLogger
from fate_arch.computing.spark import Table
from fate_arch.computing.spark._materialize import materialize
from fate_arch.protobuf.python import firework_transfer_pb2

LOGGER = getLogger()
# default message max size in bytes = 1MB
DEFAULT_MESSAGE_MAX_SIZE = 104857
NAME_DTYPE_TAG = '<dtype>'
_SPLIT_ = '^'


class FederationDataType(object):
    OBJECT = 'obj'
    TABLE = 'Table'


maximun_message_size = 66535


class _TopicPair(object):
    def __init__(self, namespace, send, receive):
        self.namespace = namespace
        self.send = send
        self.receive = receive


class FireworkChannel(object):
    def __init__(self, host, port, party_id, role, send_topic, receive_topic, send_stub,
                 query_stub: FireworkQueueServiceStub, consume_stub: FireworkQueueServiceStub = None):
        self._host = host
        self._port = port
        self._send_topic = send_topic
        self._receive_topic = receive_topic
        self._party_id = party_id
        self._role = role
        self._send_stub = send_stub
        self._consume_stub = consume_stub
        self._query_stub = query_stub

    def consume_unary(self, transferId, startOffset, session_id):
        response = self._consume_stub.consumeUnary(
            firework_transfer_pb2.ConsumeRequest(transferId=transferId, startOffset=startOffset, sessionId=session_id))
        return response

    def consume(self, transferId):
        response = self._consume_stub.consume(firework_transfer_pb2.ConsumeRequest(transferId=transferId))
        return response

    def cancel(self, transferId, session_id):
        response = self._consume_stub.cancelTransfer(
            firework_transfer_pb2.CancelTransferRequest(transferId=transferId, sessionId=session_id))
        return response

    def query(self, transfer_id, session_id):
        LOGGER.debug(f"try to query {transfer_id} session {session_id}")
        response = self._query_stub.queryTransferQueueInfo(
            firework_transfer_pb2.QueryTransferQueueInfoRequest(transferId=transfer_id, sessionId=session_id))
        return response

    def produce(self, produceRequest: firework_transfer_pb2.ProduceRequest):
        result = self._send_stub.produceUnary(produceRequest)
        return result

    # def gen(self, packages):
    #     for packet in packages:
    #         yield packet


class Datastream(object):
    def __init__(self):
        self._string = io.StringIO()
        self._string.write("[")

    def get_size(self):
        return sys.getsizeof(self._string.getvalue())

    def get_data(self):
        self._string.write("]")
        return self._string.getvalue()

    def append(self, kv: dict):
        # add ',' if not the first element
        if self._string.getvalue() != "[":
            self._string.write(",")
        json.dump(kv, self._string)

    def clear(self):
        self._string.close()
        self.__init__()


class Federation(FederationABC):

    @staticmethod
    def from_conf(federation_session_id: str,
                  party: Party,
                  runtime_conf: dict,
                  firework_config: dict):
        LOGGER.debug(f"firework_config: {firework_config}")
        return Federation(federation_session_id, party, firework_config)

    def __init__(self, session_id, party: Party, firework_config: dict):
        self._session_id = session_id
        self._party = party
        self._firework_config = firework_config

        self._topic_map = {}
        self._channels_map = {}

        self._name_dtype_map = {}
        self._message_cache = {}
        # self._max_message_size = max_message_size
        # self._topic_ttl = topic_ttl

    def __getstate__(self):
        pass

    def get(self, name: str, tag: str, parties: typing.List[Party], gc: GarbageCollectionABC) -> typing.List:
        log_str = f"[firework.get](name={name}, tag={tag}, parties={parties})"
        LOGGER.debug(f"[{log_str}]start to get")

        _name_dtype_keys = [_SPLIT_.join(
            [party.role, party.party_id, name, tag, 'get']) for party in parties]

        if _name_dtype_keys[0] not in self._name_dtype_map:
            party_topic_infos = self._get_party_topic_infos(
                parties, dtype=NAME_DTYPE_TAG)
            channel_infos = self._get_channels(
                party_topic_infos=party_topic_infos)
            rtn_dtype = []
            for i, info in enumerate(channel_infos):
                obj = self._receive_obj(
                    info, name, tag=_SPLIT_.join([tag, NAME_DTYPE_TAG]))
                rtn_dtype.append(obj)
                LOGGER.debug(f"[firework.get] _name_dtype_keys: {_name_dtype_keys}, dtype: {obj}")

            for k in _name_dtype_keys:
                if k not in self._name_dtype_map:
                    self._name_dtype_map[k] = rtn_dtype[0]
        LOGGER.info(f"_name_dtype_map:{self._name_dtype_map}")
        rtn_dtype = self._name_dtype_map[_name_dtype_keys[0]]

        rtn = []
        dtype = rtn_dtype.get("dtype", None)
        partitions = rtn_dtype.get("partitions", None)

        if dtype == FederationDataType.TABLE:
            party_topic_info = self._get_party_topic_infos(
                parties, name, partitions=partitions)
            for i in range(len(party_topic_info)):
                party = parties[i]
                role = party.role
                party_id = party.party_id
                topic_infos = party_topic_info[i]
                receive_func = self._get_partition_receive_func(name, tag, party_id, role, topic_infos,
                                                                self._session_id, conf=self._firework_config)

                sc = SparkContext.getOrCreate()
                rdd = sc.parallelize(range(partitions), partitions)
                rdd = rdd.mapPartitionsWithIndex(receive_func)
                rdd = materialize(rdd)
                table = Table(rdd)
                rtn.append(table)
                # add gc
                gc.add_gc_action(tag, table, '__del__', {})

                LOGGER.debug(
                    f"[{log_str}]received rdd({i + 1}/{len(parties)}), party: {parties[i]} ")
        else:
            party_topic_infos = self._get_party_topic_infos(parties, name)
            channel_infos = self._get_channels(
                party_topic_infos=party_topic_infos)
            for i, info in enumerate(channel_infos):
                obj = self._receive_obj(info, name, tag)
                LOGGER.debug(
                    f"[{log_str}]received obj({i + 1}/{len(parties)}), party: {parties[i]} ")
                rtn.append(obj)

        LOGGER.debug(f"[{log_str}]finish to get")
        return rtn

    def remote(self, v, name: str, tag: str, parties: typing.List[Party],
               gc: GarbageCollectionABC) -> typing.NoReturn:
        log_str = f"[firework.remote](name={name}, tag={tag}, parties={parties})"

        _name_dtype_keys = [_SPLIT_.join(
            [party.role, party.party_id, name, tag, 'remote']) for party in parties]

        # tell the receiver what sender is going to send.

        if _name_dtype_keys[0] not in self._name_dtype_map:
            party_topic_infos = self._get_party_topic_infos(
                parties, dtype=NAME_DTYPE_TAG)
            channel_infos = self._get_channels(
                party_topic_infos=party_topic_infos)
            if isinstance(v, Table):
                body = {"dtype": FederationDataType.TABLE,
                        "partitions": v.partitions}
            else:
                body = {"dtype": FederationDataType.OBJECT}

            LOGGER.debug(
                f"[firework.remote] _name_dtype_keys: {_name_dtype_keys}, dtype: {body}")
            self._send_obj(name=name, tag=_SPLIT_.join([tag, NAME_DTYPE_TAG]),
                           data=p_dumps(body), channel_infos=channel_infos, session_id=self._session_id, is_over=True)

            for k in _name_dtype_keys:
                if k not in self._name_dtype_map:
                    self._name_dtype_map[k] = body

        if isinstance(v, Table):
            total_size = v.count()
            partitions = v.partitions
            LOGGER.debug(
                f"[{log_str}]start to remote RDD, total_size={total_size}, partitions={partitions}")

            party_topic_infos = self._get_party_topic_infos(
                parties, name, partitions=partitions)
            # add gc
            gc.add_gc_action(tag, v, '__del__', {})

            send_func = self._get_partition_send_func(name, tag, partitions, party_topic_infos,
                                                      conf=self._firework_config, session_id=self._session_id)
            # noinspection PyProtectedMember
            v._rdd.mapPartitionsWithIndex(send_func).count()
        else:
            LOGGER.debug(f"[{log_str}]start to remote obj")
            party_topic_infos = self._get_party_topic_infos(parties, name)
            channel_infos = self._get_channels(
                party_topic_infos=party_topic_infos)
            self._send_obj(name=name, tag=tag, data=p_dumps(v),
                           channel_infos=channel_infos, session_id=self._session_id, is_over=True)

        LOGGER.debug(f"[{log_str}]finish to remote")

    def cleanup(self, parties):
        LOGGER.debug("[firework.cleanup]start to cleanup...")

    def _get_party_topic_infos(self, parties: typing.List[Party], name=None, partitions=None,
                               dtype=None) -> typing.List:
        topic_infos = [self._get_or_create_topic(
            party, name, partitions, dtype) for party in parties]

        # the return is formed like this: [[(topic_key1, topic_info1), (topic_key2, topic_info2)...],[(topic_key1, topic_info1), (topic_key2, topic_info2]...]
        return topic_infos

    def _get_or_create_topic(self, party: Party, name=None, partitions=None, dtype=None,
                             client_type=None) -> typing.List:
        topic_key_list = []
        topic_infos = []

        if dtype is not None:
            topic_key = _SPLIT_.join(
                [party.role, party.party_id, dtype, dtype])
            topic_key_list.append(topic_key)
        else:
            if partitions is not None:
                for i in range(partitions):
                    topic_key = _SPLIT_.join(
                        [party.role, party.party_id, name, str(i)])
                    topic_key_list.append(topic_key)
            elif name is not None:
                topic_key = _SPLIT_.join([party.role, party.party_id, name])
                topic_key_list.append(topic_key)
            else:
                topic_key = _SPLIT_.join([party.role, party.party_id])
                topic_key_list.append(topic_key)

        for topic_key in topic_key_list:
            if topic_key not in self._topic_map:
                LOGGER.debug(
                    f"[firework.get_or_create_topic]topic: {topic_key} for party:{party} not found, start to create")
                # gen names
                topic_key_splits = topic_key.split(_SPLIT_)
                queue_suffix = "-".join(topic_key_splits[2:])
                send_topic_name = f"{self._session_id}-{self._party.role}-{self._party.party_id}-{party.role}-{party.party_id}-{queue_suffix}"
                receive_topic_name = f"{self._session_id}-{party.role}-{party.party_id}-{self._party.role}-{self._party.party_id}-{queue_suffix}"

                # topic_pair is a pair of topic for sending and receiving message respectively
                topic_pair = _TopicPair(
                    namespace=self._session_id, send=send_topic_name, receive=receive_topic_name)
                self._topic_map[topic_key] = topic_pair
                LOGGER.debug(f"[firework.get_or_create_topic]topic for topic_key: {topic_key}, party:{party} created")
            topic_pair = self._topic_map[topic_key]
            topic_infos.append((topic_key, topic_pair))
        return topic_infos

    def _get_channel(self, topic_pair, party_id, role, conf: dict):
        host = conf.get('host')
        port = conf.get('port')
        print(f"get_channel {host}:{port}")
        channel = grpc.insecure_channel('{}:{}'.format(host, port))
        send_stub = FireworkQueueServiceStub(channel)
        query_stub = FireworkQueueServiceStub(channel)

        return FireworkChannel(host, port, party_id, role, topic_pair.send, topic_pair.receive, send_stub, query_stub)

    def _get_channels(self, party_topic_infos):
        channel_infos = []
        for e in party_topic_infos:
            for topic_key, topic_pair in e:
                topic_key_splits = topic_key.split(_SPLIT_)
                role = topic_key_splits[0]
                party_id = topic_key_splits[1]
                info = self._channels_map.get(topic_key)
                if info is None:
                    info = self._get_channel(topic_pair, party_id=party_id, role=role,
                                             conf=self._firework_config)
                    self._channels_map[topic_key] = info
                channel_infos.append(info)
        return channel_infos

    def _send_obj(self, name, tag, data, channel_infos, session_id, is_over=False):
        for info in channel_infos:
            # selfmade properties
            properties = {
                'content_type': 'text/plain',
                'app_id': info._party_id,
                'message_id': name,
                'correlation_id': tag,
            }
            LOGGER.info(f"send topic: {info._send_topic}")
            package = self._encode_packet(info._send_topic, info._party_id, info._role, data, properties, session_id,
                                          is_over)
            response = info.produce(package)
            if response.code != 0:
                raise ValueError(f"firework response code {response.code}")

    def _encode_packet(self, topic, party_id, role, data, properties, session_id, is_over=False):

        return firework_transfer_pb2.ProduceRequest(transferId=topic, sessionId=session_id,
                                                    routeInfo=firework_transfer_pb2.RouteInfo(srcPartyId="9999",
                                                                                              srcRole="test",
                                                                                              desPartyId=party_id,
                                                                                              desRole="ccc"),
                                                    message=firework_transfer_pb2.Message(
                                                        head=bytes(json.dumps(properties), encoding="utf-8"),
                                                        body=data), isOver=is_over)

    def _get_message_cache_key(self, name, tag, party_id, role):
        cache_key = _SPLIT_.join([name, tag, str(party_id), role])
        return cache_key

    def _receive_obj(self, channel_info, name, tag):
        party_id = channel_info._party_id
        role = channel_info._role
        LOGGER.info(f"channel info {channel_info._receive_topic}")
        wish_cache_key = self._get_message_cache_key(name, tag, party_id, role)
        LOGGER.info(f"message cache len: {len(self._message_cache.keys())}, values:{self._message_cache}")
        self._query_topic_info(channel_info, channel_info._receive_topic, self._session_id)

        try_count = 0
        while True:
            if wish_cache_key in self._message_cache:
                LOGGER.info(f"get wish_cache_key from _message_cache")
                return self._message_cache[wish_cache_key]
            try_count = try_count + 1
            # return None indicates the client is closed
            response = channel_info.consume_unary(channel_info._receive_topic, -1, self._session_id)
            LOGGER.info(f"response code {response.code}")
            if response.code == 0:
                message = response.message
                head_str = str(message.head, encoding="utf-8")
                LOGGER.debug(f"head str {head_str}")
                properties = json.loads(head_str)
                LOGGER.info(f"firework response propertiest {properties}")
                body = message.body
                if properties['message_id'] != name or properties['correlation_id'] != tag:
                    LOGGER.warning(
                        f"[firework._receive_obj] require {name}.{tag}, got {properties['message_id']}.{properties['correlation_id']}")
                    # just ack and continue
                    # channel_info.basic_ack(message)
                    continue

                cache_key = self._get_message_cache_key(
                    properties['message_id'], properties['correlation_id'], party_id, role)
                # object
                if properties['content_type'] == 'text/plain':
                    self._message_cache[cache_key] = p_loads(body)
                    # channel_info.basic_ack(message)
                    if cache_key == wish_cache_key:
                        channel_info.cancel(channel_info._receive_topic, self._session_id)
                        LOGGER.debug(
                            f"[firework._receive_obj] cache_key: {cache_key}, obj: {self._message_cache[cache_key]}")
                        return self._message_cache[cache_key]
                    else:
                        LOGGER.debug(
                            f"kaideng cache_key: {cache_key}, wish: {wish_cache_key}")

                else:
                    raise ValueError(
                        f"[firework._receive_obj] properties.content_type is {properties.content_type}, but must be text/plain")
            else:
                LOGGER.info(f"try count: {try_count}")
                time.sleep(5)
                if try_count > 30:
                    raise ValueError(
                        f"{channel_info._receive_topic} try over time")

    def _send_kv(self, name, tag, data, channel_infos, partition_size, partitions, message_key, session_id,
                 is_over=False):
        headers = json.dumps({"partition_size": partition_size,
                              "partitions": partitions, "message_key": message_key})

        for info in channel_infos:
            properties = {
                'content_type': 'application/json',
                'app_id': info._party_id,
                'message_id': name,
                'correlation_id': tag,
                'headers': headers
            }
            print(
                f"[firework._send_kv]info: {info}, properties: {properties}.")
            package = self._encode_packet(info._send_topic, info._party_id, info._role, data, properties, session_id,
                                          is_over)
            response = info.produce(package)
            if response.code != 0:
                raise ValueError(f"firework response code {response.code}")

    def _get_partition_send_func(self, name, tag, partitions, party_topic_infos, conf: dict, session_id):
        def _fn(index, kvs):
            return self._partition_send(index, kvs, name, tag, partitions, party_topic_infos, conf, session_id)

        return _fn

    def _get_channels_index(self, index, party_topic_infos, conf: dict):
        channel_infos = []
        for e in party_topic_infos:
            # select specified topic_info for a party
            topic_key, topic_pair = e[index]
            topic_key_splits = topic_key.split(_SPLIT_)
            role = topic_key_splits[0]
            party_id = topic_key_splits[1]
            info = self._get_channel(
                topic_pair, party_id=party_id, role=role, conf=conf)
            channel_infos.append(info)
        return channel_infos

    def _partition_send(self, index, kvs, name, tag, partitions, party_topic_infos, conf: dict, session_id):
        channel_infos = self._get_channels_index(
            index=index, party_topic_infos=party_topic_infos, conf=conf)

        datastream = Datastream()
        base_message_key = str(index)
        message_key_idx = 0
        count = 0

        for k, v in kvs:
            count += 1
            el = {'k': p_dumps(k).hex(), 'v': p_dumps(v).hex()}
            if datastream.get_size() + sys.getsizeof(el['k']) + sys.getsizeof(el['v']) >= maximun_message_size:
                print(f'[firework._partition_send]The size of message is: {datastream.get_size()}')
                message_key_idx += 1
                message_key = _SPLIT_.join([base_message_key, str(message_key_idx)])
                self._send_kv(name=name, tag=tag, data=datastream.get_data().encode(), channel_infos=channel_infos,
                              partition_size=-1, partitions=partitions, message_key=message_key, session_id=session_id)
                datastream.clear()
            datastream.append(el)

        message_key_idx += 1
        message_key = _SPLIT_.join([base_message_key, str(message_key_idx)])

        self._send_kv(name=name, tag=tag, data=datastream.get_data().encode(), channel_infos=channel_infos,
                      partition_size=count, partitions=partitions, message_key=message_key, session_id=session_id,
                      is_over=True)

        return [1]

    def _get_partition_receive_func(self, name, tag, party_id, role, topic_infos, session_id, conf: dict):
        def _fn(index, kvs):
            return self._partition_receive(index, kvs, name, tag, party_id, role, topic_infos, session_id, conf)

        return _fn

    def _query_topic_info(self, channel_info, receive_topic, session_id):
        needRetry = True
        retryCount = 0
        while needRetry and retryCount < 30:
            retryCount = retryCount + 1
            response = channel_info.query(receive_topic, session_id)

            if response.code == 0:
                needRetry = False
                host = response.transferQueueInfo[0].ip
                port = response.transferQueueInfo[0].port
                LOGGER.info(f"query {receive_topic} result {host} {port}");
                channel = grpc.insecure_channel('{}:{}'.format(host, port))
                channel_info._consume_stub = FireworkQueueServiceStub(channel)
                break
            else:
                if response.code == 138:
                    time.sleep(5)
                else:
                    raise ValueError(f"query topic info {receive_topic} error code {response.code}")

    def _handle(i):
        LOGGER.info(f"receive {i}")

    def _partition_receive(self, index, kvs, name, tag, party_id, role, topic_infos, session_id, conf: dict):
        LOGGER.info(f"partition receive {index} {kvs} {name} {tag} {party_id} {role} {topic_infos} {conf}")
        topic_pair = topic_infos[index][1]
        channel_info = self._get_channel(
            topic_pair, party_id, role, conf)
        message_key_cache = set()
        all_data = []
        count = 0

        if not channel_info._consume_stub:
            self._query_topic_info(channel_info, topic_pair.receive, session_id)

        try_count = 0
        partition_size = -1

        while True:
            try_count = try_count + 1
            response = channel_info.consume_unary(topic_pair.receive, -1, session_id)
            if response.code == 0:
                message = response.message
                head_str = str(message.head, encoding="utf-8")
                properties = json.loads(head_str)
                body = message.body
                LOGGER.info(
                    f"properties: {properties}.")
                if properties['message_id'] != name or properties['correlation_id'] != tag:
                    # leave this code to handle unexpected situation
                    # channel_info.basic_ack(message)
                    LOGGER.info(
                        f"[firework._partition_receive]: require {name}.{tag}, got {properties['message_id']}.{properties['correlation_id']}")
                    continue

                if properties['content_type'] == 'application/json':
                    # headers here is json bytes string
                    header = json.loads(properties['headers'])
                    message_key = header.get('message_key')
                    if message_key in message_key_cache:
                        LOGGER.info(
                            f"[firework._partition_receive] message_key : {message_key} is duplicated")
                        # channel_info.basic_ack(message)
                        continue
                    message_key_cache.add(message_key)
                    if header.get('partition_size') >= 0:
                        partition_size = header.get('partition_size')

                    data = json.loads(body)
                    data_iter = ((p_loads(bytes.fromhex(el['k'])), p_loads(
                        bytes.fromhex(el['v']))) for el in data)
                    count += len(data)
                    LOGGER.info(f"[firework._partition_receive] count: {count}")
                    all_data.extend(data_iter)
                    # channel_info.basic_ack(message)

                    if count == partition_size:
                        channel_info.cancel(topic_pair.receive, session_id)
                        return all_data
                else:
                    raise ValueError(
                        f"[firework._partition_receive]properties.content_type is {properties.content_type}, but must be application/json")
            else:
                LOGGER.error(f"firework response {response} try count {try_count}")
                time.sleep(1)
                if try_count > 20:
                    raise ValueError(
                        f"firework can not get message")
