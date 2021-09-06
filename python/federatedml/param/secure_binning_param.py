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

from federatedml.param.base_param import BaseParam
from federatedml.param.feature_binning_param import TransformParam
from federatedml.util import consts
import copy


class FeatureBinningParam(BaseParam):
    """
    Define the feature binning method

    Parameters
    ----------
    method : str, 'quantile'， 'bucket' or 'optimal', default: 'quantile'
        Binning method.

    compress_thres: int, default: 10000
        When the number of saved summaries exceed this threshold, it will call its compress function

    head_size: int, default: 10000
        The buffer size to store inserted observations. When head list reach this buffer size, the
        QuantileSummaries object start to generate summary(or stats) and insert into its sampled list.

    error: float, 0 <= error < 1 default: 0.001
        The error of tolerance of binning. The final split point comes from original data, and the rank
        of this value is close to the exact rank. More precisely,
        floor((p - 2 * error) * N) <= rank(x) <= ceil((p + 2 * error) * N)
        where p is the quantile in float, and N is total number of data.

    bin_num: int, bin_num > 0, default: 10
        The max bin number for binning

    bin_indexes : list of int or int, default: -1
        Specify which columns need to be binned. -1 represent for all columns. If you need to indicate specific
        cols, provide a list of header index instead of -1.

    bin_names : list of string, default: []
        Specify which columns need to calculated. Each element in the list represent for a column name in header.

    adjustment_factor : float, default: 0.5
        the adjustment factor when calculating WOE. This is useful when there is no event or non-event in
        a bin. Please note that this parameter will NOT take effect for setting in host.

    category_indexes : list of int or int, default: []
        Specify which columns are category features. -1 represent for all columns. List of int indicate a set of
        such features. For category features, bin_obj will take its original values as split_points and treat them
        as have been binned. If this is not what you expect, please do NOT put it into this parameters.

        The number of categories should not exceed bin_num set above.

    category_names : list of string, default: []
        Use column names to specify category features. Each element in the list represent for a column name in header.

    local_only : bool, default: False
        Whether just provide binning method to guest party. If true, host party will do nothing.
        Warnings: This parameter will be deprecated in future version.

    transform_param: TransformParam
        Define how to transfer the binned data.

    need_run: bool, default True
        Indicate if this module needed to be run

    skip_static: bool, default False
        If true, binning will not calculate iv, woe etc. In this case, optimal-binning
        will not be supported.

    """

    def __init__(self, method=consts.QUANTILE,
                 compress_thres=consts.DEFAULT_COMPRESS_THRESHOLD,
                 head_size=consts.DEFAULT_HEAD_SIZE,
                 error=consts.DEFAULT_RELATIVE_ERROR,
                 bin_num=consts.G_BIN_NUM, bin_indexes=-1, bin_names=None, adjustment_factor=0.5,
                 transform_param=TransformParam(),
                 local_only=False,
                 category_indexes=None, category_names=None,
                 need_run=True, skip_static=False):
        super(FeatureBinningParam, self).__init__()
        self.method = method
        self.compress_thres = compress_thres
        self.head_size = head_size
        self.error = error
        self.adjustment_factor = adjustment_factor
        self.bin_num = bin_num
        self.bin_indexes = bin_indexes
        self.bin_names = bin_names
        self.category_indexes = category_indexes
        self.category_names = category_names
        self.transform_param = copy.deepcopy(transform_param)
        self.need_run = need_run
        self.skip_static = skip_static
        self.local_only = local_only

    def check(self):
        descr = "Binning param's"
        self.check_string(self.method, descr)
        self.method = self.method.lower()
        self.check_positive_integer(self.compress_thres, descr)
        self.check_positive_integer(self.head_size, descr)
        self.check_decimal_float(self.error, descr)
        self.check_positive_integer(self.bin_num, descr)
        if self.bin_indexes != -1:
            self.check_defined_type(self.bin_indexes, descr, ['list', 'RepeatedScalarContainer', "NoneType"])
        self.check_defined_type(self.bin_names, descr, ['list', "NoneType"])
        self.check_defined_type(self.category_indexes, descr, ['list', "NoneType"])
        self.check_defined_type(self.category_names, descr, ['list', "NoneType"])
        self.check_open_unit_interval(self.adjustment_factor, descr)
        self.check_boolean(self.local_only, descr)


class HeteroFeatureBinningParam(FeatureBinningParam):
    def __init__(self, method=consts.QUANTILE,
                 compress_thres=consts.DEFAULT_COMPRESS_THRESHOLD,
                 head_size=consts.DEFAULT_HEAD_SIZE,
                 error=consts.DEFAULT_RELATIVE_ERROR,
                 bin_num=consts.G_BIN_NUM, bin_indexes=-1, bin_names=None, adjustment_factor=0.5,
                 transform_param=TransformParam(), optimal_binning_param=OptimalBinningParam(),
                 local_only=False, category_indexes=None, category_names=None,
                 encrypt_param=EncryptParam(),
                 need_run=True, skip_static=False):
        super(HeteroFeatureBinningParam, self).__init__(method=method, compress_thres=compress_thres,
                                                        head_size=head_size, error=error,
                                                        bin_num=bin_num, bin_indexes=bin_indexes,
                                                        bin_names=bin_names, adjustment_factor=adjustment_factor,
                                                        transform_param=transform_param,
                                                        category_indexes=category_indexes,
                                                        category_names=category_names,
                                                        need_run=need_run, local_only=local_only,
                                                        skip_static=skip_static)
        self.optimal_binning_param = copy.deepcopy(optimal_binning_param)
        self.encrypt_param = encrypt_param

    def check(self):
        descr = "Hetero Binning param's"
        super(HeteroFeatureBinningParam, self).check()
        self.check_valid_value(self.method, descr, [consts.QUANTILE, consts.BUCKET, consts.OPTIMAL])
        self.optimal_binning_param.check()
        self.encrypt_param.check()
        if self.encrypt_param.method != consts.PAILLIER:
            raise ValueError("Feature Binning support Paillier encrypt method only.")
        if self.skip_static and self.method == consts.OPTIMAL:
            raise ValueError("When skip_static, optimal binning is not supported.")
        self.transform_param.check()
        if self.skip_static and self.transform_param.transform_type == 'woe':
            raise ValueError("To use woe transform, skip_static should set as False")