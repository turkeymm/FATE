#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
#  Copyright 2019 The FATE Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from fate_arch.federation import Tag, get, remote
from fate_arch.session import PartiesInfo as Parties
from federatedml.util import LOGGER
from federatedml.util import consts


class Guest(object):
    def __init__(self, has_arbiter):
        self.has_arbiter = has_arbiter

    @Tag("Batch")
    def sync_batch_info(self, batch_info):
        # self.batch_data_info_transfer.remote(obj=batch_info,
        #                                      role=consts.HOST,
        #                                      suffix=suffix)

        remote(parties=Parties.Host[:], name="batch_info", v=batch_info)

        if self.has_arbiter:
            # self.batch_data_info_transfer.remote(obj=batch_info,
            #                                      role=consts.ARBITER,
            #                                      suffix=suffix)
            remote(parties=Parties.Arbiter[:], name="batch_info", v=batch_info)

    @Tag("Batch")
    def sync_batch_index(self, batch_data_index, batch_index):
        # self.batch_data_index_transfer.remote(obj=batch_index,
        #                                       role=consts.HOST,
        #                                       suffix=suffix)

        remote(parties=Parties.Host[:], name="batch_index-" + str(batch_index), v=batch_data_index)

    @Tag("Batch")
    def sync_batch_validate_info(self):
        # if not self.batch_validate_info_transfer:
        #     raise ValueError("batch_validate_info should be create in transfer variable")

        # validate_info = self.batch_validate_info_transfer.get(idx=-1,
        #                                                       suffix=suffix)

        validate_info = get(parties=Parties.Host[:], name="validate_info")
        return validate_info


class Host(object):
    @Tag("Batch")
    def sync_batch_info(self):
        # LOGGER.debug("In sync_batch_info, suffix is :{}".format(suffix))
        # batch_info = self.batch_data_info_transfer.get(idx=0,
        #                                                suffix=suffix)
        batch_info = get(parties=Parties.Guest[0], name="batch_info")
        batch_size = batch_info.get('batch_size')
        if batch_size < consts.MIN_BATCH_SIZE and batch_size != -1:
            raise ValueError(
                "Batch size get from guest should not less than {}, except -1, batch_size is {}".format(
                    consts.MIN_BATCH_SIZE, batch_size))
        return batch_info

    @Tag("Batch")
    def sync_batch_index(self, batch_index):
        # batch_index = self.batch_data_index_transfer.get(idx=0,
        #                                                  suffix=suffix)

        batch_index = get(parties=Parties.Guest[0], name="batch_index-" + str(batch_index))

        return batch_index

    @Tag("Batch")
    def sync_batch_validate_info(self, validate_info):
        # self.batch_validate_info_transfer.remote(obj=validate_info,
        #                                          role=consts.GUEST,
        #                                          suffix=suffix)

        remote(parties=Parties.Guest[:], name="validate_info", v=validate_info)


class Arbiter(object):
    @Tag("Batch")
    def sync_batch_info(self):
        # batch_info = self.batch_data_info_transfer.get(idx=0,
        #                                                suffix=suffix)
        batch_info = get(parties=Parties.Guest[0], name="batch_info")

        return batch_info
