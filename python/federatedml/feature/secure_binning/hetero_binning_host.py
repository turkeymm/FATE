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
#

import functools
import operator
import random
import copy

import numpy as np

from federatedml.cipher_compressor import compressor
from federatedml.feature.secure_binning.base_feature_binning import SecureBinningBase
from federatedml.secureprotol.fate_paillier import PaillierEncryptedNumber
from federatedml.util import LOGGER
from federatedml.util import consts


class HeteroFeatureBinningHost(SecureBinningBase):
    def fit(self, data_instances):
        """
        Apply binning method for both data instances in local party as well as the other one. Afterwards, calculate
        the specific metric value for specific columns.
        """
        self._abnormal_detection(data_instances)

        # self._parse_cols(data_instances)
        self._setup_bin_inner_param(data_instances, self.model_param)

        # Calculates split points of datas in self party
        split_points = self.binning_obj.fit_split_points(data_instances)
        if self.model_param.skip_static or self.is_local_only:
            self.transform(data_instances)
            return self.data_output

        encrypted_label_table = self.prepare_labels(data_instances)
        if encrypted_label_table is None:
            self.transform(data_instances)
            total_summary = self.binning_obj.bin_results.to_json()
            self.set_summary(total_summary)
            return data_instances

        self.cal_woe(data_instances, split_points, encrypted_label_table)

        self._sync_init_bucket(data_instances, split_points, encrypted_label_table)
        if self.model_param.method == consts.OPTIMAL:
            self.optimal_binning_sync()

        if self.transform_type != 'woe':
            data_instances = self.transform(data_instances)
        self.set_schema(data_instances)
        self.data_output = data_instances
        total_summary = self.binning_obj.bin_results.to_json()
        self.set_summary(total_summary)
        return data_instances

    def _sync_init_bucket(self, data_instances, split_points, need_shuffle=False):

        data_bin_table = self.binning_obj.get_data_bin(data_instances, split_points)
        LOGGER.debug("data_bin_table, count: {}".format(data_bin_table.count()))

        encrypted_label_table = self.transfer_variable.encrypted_label.get(idx=0)

        self._compute_woe(data_bin_table, encrypted_label_table, split_points)

        LOGGER.info("Get encrypted_label_table from guest")

        encrypted_bin_sum = self.__static_encrypted_bin_label(data_bin_table, encrypted_label_table,
                                                              self.bin_inner_param.bin_cols_map, split_points)

        encode_name_f = functools.partial(self.bin_inner_param.encode_col_name_dict,
                                          model=self,
                                          col_name_maps=self.bin_inner_param.col_name_maps)
        # encrypted_bin_sum = self.bin_inner_param.encode_col_name_dict(encrypted_bin_sum, self)
        encrypted_bin_sum = encrypted_bin_sum.map(encode_name_f)

        self.header_anonymous = self.bin_inner_param.encode_col_name_list(self.header, self)
        encrypted_bin_sum = self.cipher_compress(encrypted_bin_sum, data_bin_table.count())
        self.transfer_variable.encrypted_bin_sum.remote(encrypted_bin_sum,
                                                        role=consts.GUEST,
                                                        idx=0)
        send_result = {
            "category_names": self.bin_inner_param.encode_col_name_list(
                self.bin_inner_param.category_names, self),
            "bin_method": self.model_param.method,
            "optimal_params": {
                "metric_method": self.model_param.optimal_binning_param.metric_method,
                "bin_num": self.model_param.bin_num,
                "mixture": self.model_param.optimal_binning_param.mixture,
                "max_bin_pct": self.model_param.optimal_binning_param.max_bin_pct,
                "min_bin_pct": self.model_param.optimal_binning_param.min_bin_pct
            }
        }
        self.transfer_variable.optimal_info.remote(send_result,
                                                   role=consts.GUEST,
                                                   idx=0)

    def _compute_woe(self, data_bin_table, encrypted_label_table, split_points):
        # Make confuse bin
        confused_table = self._create_confuse_bin(data_bin_table, split_points)
        encrypted_bin = self.__static_encrypted_bin_label(data_bin_table, encrypted_label_table,
                                                          self.bin_inner_param.bin_cols_map, split_points)
        encrypted_confused_bin = self.__static_encrypted_bin_label(confused_table, encrypted_label_table,
                                                                   self.bin_inner_param.bin_cols_map, split_points)
        confused_table = encrypted_bin.join(encrypted_confused_bin, self._merge_confused_bin)
        # assert 1 == 2, f"enctyped_bin: {encrypted_bin.first()}"

        encrypted_bin_sum = self._apply_random_num(encrypted_bin_sum)
        # remote encrypted_bin_sum
        # received log(A*r1) - log(B*r2)
        # Cal woe
        # remote back


    def _apply_random_num(self, encrypted_bin_sum):
        pass

    def optimal_binning_sync(self):
        bucket_idx = self.transfer_variable.bucket_idx.get(idx=0)
        LOGGER.debug("In optimal_binning_sync, received bucket_idx: {}".format(bucket_idx))
        original_split_points = self.binning_obj.bin_results.all_split_points
        for encoded_col_name, b_idx in bucket_idx.items():
            col_name = self.bin_inner_param.decode_col_name(encoded_col_name)
            ori_sp_list = original_split_points.get(col_name)
            optimal_result = [ori_sp_list[i] for i in b_idx]
            self.binning_obj.bin_results.put_col_split_points(col_name, optimal_result)
