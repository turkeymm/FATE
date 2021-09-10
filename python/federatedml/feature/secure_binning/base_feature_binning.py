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

import copy
import random
import functools

import numpy as np

from federatedml.feature.hetero_feature_binning.base_feature_binning import BaseFeatureBinning
from federatedml.secureprotol import PaillierEncrypt
from federatedml.statistic import data_overview
from federatedml.util import consts
from federatedml.feature.binning.iv_calculator import IvCalculator

MODEL_PARAM_NAME = 'SecureFeatureBinningParam'
MODEL_META_NAME = 'SecureFeatureBinningMeta'


class SecureBinningBase(BaseFeatureBinning):
    """
    Do binning method through guest and host
    """

    def __init__(self):
        super().__init__()
        self.random_filed = 1 << 6

    def _guest_prepare_labels(self, data_instances):
        label_counts_dict = data_overview.get_label_count(data_instances)

        if len(label_counts_dict) > 2:
            raise ValueError("Secure mode binning does not support multi-class data yet")
        self.labels = list(label_counts_dict.keys())
        label_counts = [label_counts_dict[k] for k in self.labels]
        # label_table = data_instances.mapValues(lambda x: x.label)
        label_table = IvCalculator.convert_label(data_instances, self.labels)

        if not self.is_local_only:
            encrypted_label_table = label_table.mapValues(lambda x: self.cipher.recursive_encrypt(x))
            self.transfer_variable.encrypted_label.remote(encrypted_label_table,
                                                          role=consts.HOST,
                                                          idx=-1)
        return label_counts_dict, label_counts, label_table

    def _host_prepare_labels(self):
        if not self.is_local_only:
            return self.transfer_variable.encrypted_label.get(idx=0)
        return None

    def cal_woe(self, data_instances, split_points, encrypted_label_table=None):
        if self.role == consts.GUEST:
            self._guest_woe_compute()
        else:
            self._host_woe_compute(data_instances, split_points, encrypted_label_table)

    def _guest_woe_compute(self):
        confused_table = self.transfer_variable.confused_table.get(idx=-1)
        def _compute_log_ratio(static_count, cipher):
            ratio = []
            for event_count, non_event_count in static_count:
                ratio.append(np.log(cipher.decrypt(event_count)) -
                             np.log(cipher.decrypt(non_event_count)))
            return np.ar
        decrypted_table = confused_table.mapValues(lambda x: self.cipher.recursive_decrypt(x))

    def _host_woe_compute(self, data_instances, split_points, encrypted_label_table):
        data_bin_table = self.binning_obj.get_data_bin(data_instances,
                                                       split_points,
                                                       self.bin_inner_param.bin_cols_map)
        confused_table = self._create_confuse_bin(data_bin_table, split_points)
        encrypted_bin = self._static_encrypted_bin_label(data_bin_table,
                                                         encrypted_label_table,
                                                         with_compress=False)
        encrypted_confused_bin = self._static_encrypted_bin_label(confused_table,
                                                                  encrypted_label_table,
                                                                  with_compress=False)
        confused_table_res = encrypted_bin.join(encrypted_confused_bin, self._merge_confused_bin)
        confused_table = confused_table_res.mapValues(lambda x: x[0])
        true_bin_index = confused_table_res.mapValues(lambda x: x[1])
        # assert 1 == 2, f"confused_table: {confused_table.first()}"
        apply_random_f = functools.partial(self._apply_random_num,
                                           random_filed=self.random_filed)
        confused_table = confused_table.mapValues(apply_random_f)
        random_table = confused_table.mapValues(lambda x: x[1])
        confused_table = confused_table.mapValues(lambda x: x[0])
        self.transfer_variable.confused_table.remote(confused_table)

    @staticmethod
    def _merge_confused_bin(true_bins, fake_bins):
        true_bin_len, fake_bin_len = len(true_bins), len(fake_bins)
        index_pool = [i for i in range(true_bin_len + fake_bin_len)]
        random.SystemRandom().shuffle(index_pool)
        true_bin_index = [index_pool.index(x) for x in range(true_bin_len)]
        res_bin = np.append(true_bins, fake_bins)[index_pool]
        # test if it can be recover
        # original_bin = res_bin[true_bin_index]
        # assert 1 == 2, f"res_bin: {res_bin}, original_bin: {original_bin}," \
        #                f"index_pool: {index_pool}, true_bin_index: {true_bin_index}"
        #
        return res_bin, true_bin_index

    @staticmethod
    def _create_confuse_bin(data_bin_table, split_points):
        bin_num = {k: len(v) for k, v in split_points.items()}

        def _make_confusion_table(bin_dict):
            res = {}
            for feature_name in bin_dict.keys():
                max_num = bin_num.get(feature_name)
                res[feature_name] = random.SystemRandom().randint(0, max_num - 1)
            return res

        confused_table = data_bin_table.mapValues(_make_confusion_table)
        return confused_table

    @staticmethod
    def _apply_random_num(static_count, random_filed):
        shape = static_count.shape()
        random_num = shape[0] * shape[1]
        random_arr = []
        for _ in range(random_num):
            random_arr.append(random.SystemRandom().randint(1, random_filed))
        random_arr = np.array(random_arr).reshape(shape)
        return static_count + random_arr, random_arr
