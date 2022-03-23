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

from fate_arch.federation import Tag, get, remote


class Communicator(object):

    def __init__(self, local_party=None, all_parties=None):
        self._local_party = local_party
        self._all_parties = all_parties
        self._party_idx = self._all_parties.index(self._local_party)
        self._other_parties = self._all_parties[:self._party_idx] + self._all_parties[(self._party_idx + 1):]

    @property
    def party(self):
        return self._local_party

    @property
    def parties(self):
        return self._all_parties

    @property
    def other_parties(self):
        return self._other_parties

    @property
    def party_idx(self):
        return self._party_idx

    @Tag("q_field")
    def remote_q_field(self, q_field,  party):
        return remote(parties=party, name="q_field", v=q_field)
        # return self._share_variable.remote_parties(share, party, suffix=(tensor_name,))

    @Tag("q_field")
    def get_q_field(self, party):
        if not isinstance(party, list):
            party = [party]
        return get(parties=party, name="q_field")
        # return self._share_variable.get_parties(party, suffix=(tensor_name,))

    @Tag("rescontruct")
    def get_rescontruct_shares(self, tensor_name):
        return get(parties=self._other_parties, name=tensor_name)
        # return self._rescontruct_variable.get_parties(self._other_parties, suffix=(tensor_name,))

    @Tag("rescontruct")
    def broadcast_rescontruct_share(self, share, tensor_name):
        return remote(parties=self._other_parties, name=tensor_name, v=share)
        # return self._rescontruct_variable.remote_parties(share, self._other_parties, suffix=(tensor_name,))

    @Tag("share")
    def remote_share(self, share, tensor_name, party):
        return remote(parties=party, name=tensor_name, v=share)
        # return self._share_variable.remote_parties(share, party, suffix=(tensor_name,))

    @Tag("share")
    def get_share(self, tensor_name, party):
        if not isinstance(party, list):
            party = [party]
        return get(parties=party, name=tensor_name)
        # return self._share_variable.get_parties(party, suffix=(tensor_name,))

    @Tag("multiply_triplets_encrypted")
    def remote_encrypted_tensor(self, encrypted, tag):
        return remote(parties=self._other_parties, name=tag, v=encrypted)
        # return self._mul_triplets_encrypted_variable.remote_parties(encrypted, parties=self._other_parties, suffix=tag)

    @Tag("multiply_triplets_cross")
    def remote_encrypted_cross_tensor(self, encrypted, parties, tag):
        return remote(parties=parties, name=tag, v=encrypted)
        # return self._mul_triplets_cross_variable.remote_parties(encrypted, parties=parties, suffix=tag)

    @Tag("multiply_triplets_encrypted")
    def get_encrypted_tensors(self, tag):
        return(
            self._other_parties,
            get(parties=self._other_parties, name=tag)
        )

        # return (self._other_parties,
        #         self._mul_triplets_encrypted_variable.get_parties(parties=self._other_parties, suffix=tag))

    @Tag("multiply_triplets_cross")
    def get_encrypted_cross_tensors(self, tag):
        return get(parties=self._other_parties, name=tag)
        # return self._mul_triplets_cross_variable.get_parties(parties=self._other_parties, suffix=tag)

    # def clean(self):
    #     self._rescontruct_variable.clean()
    #     self._share_variable.clean()
    #     self._rescontruct_variable.clean()
    #     self._mul_triplets_encrypted_variable.clean()
    #     self._mul_triplets_cross_variable.clean()

