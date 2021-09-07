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

import copy
import functools

from federatedml.cipher_compressor.packer import GuestIntegerPacker
from federatedml.feature.binning.base_binning import BaseBinning
from federatedml.feature.binning.optimal_binning.optimal_binning import OptimalBinning
from federatedml.feature.secure_binning.base_feature_binning import SecureBinningBase
from federatedml.secureprotol import PaillierEncrypt
from federatedml.secureprotol.fate_paillier import PaillierEncryptedNumber
from federatedml.statistic import data_overview
from federatedml.statistic import statics
from federatedml.util import LOGGER
from federatedml.util import consts


class HeteroFeatureBinningGuest(SecureBinningBase):


    def fit(self, data_instances):
        """
        Apply binning method for both data instances in local party as well as the other one. Afterwards, calculate
        the specific metric value for specific columns. Currently, iv is support for binary labeled data only.
        """
        LOGGER.info("Start feature binning fit and transform")
        self._abnormal_detection(data_instances)

        # self._parse_cols(data_instances)

        self._setup_bin_inner_param(data_instances, self.model_param)
        split_points = self.binning_obj.fit_split_points(data_instances)

        if self.model_param.skip_static:
            self.transform(data_instances)
            return self.data_output

        label_counts_dict, label_counts, label_table = self.prepare_labels(data_instances)

        self.bin_result = self.iv_calculator.cal_local_iv(data_instances=data_instances,
                                                          split_points=split_points,
                                                          labels=self.labels,
                                                          label_counts=label_counts,
                                                          bin_cols_map=self.bin_inner_param.get_need_cal_iv_cols_map(),
                                                          label_table=label_table)

        if self.model_param.local_only:
            self.transform(data_instances)
            self.set_summary(self.bin_result.summary())
            return self.data_output

        self.cal_woe(data_instances, split_points)


        total_summary = self.bin_result.summary()
        for host_res in self.host_results:
            total_summary = self._merge_summary(total_summary, host_res.summary())

        self.set_schema(data_instances)
        self.transform(data_instances)
        LOGGER.info("Finish feature binning fit and transform")
        self.set_summary(total_summary)
        return self.data_output

