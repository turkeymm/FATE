from ._torch import PyTorchFederatedTrainer, PyTorchSAClientContext, EarlyStopCallback, FedLightModule, make_dataset
import json
import os
import tempfile

import pytorch_lightning as pl
from torch import nn, optim
import torch
from federatedml.param import HomoNNParam
import numpy as np


class MyFedLightModule(FedLightModule):
    def __init__(
            self,
            context: PyTorchSAClientContext,
    ):
        super(FedLightModule).__init__()
        self._num_data_consumed = 0
        self._all_consumed_data_aggregated = True
        self._should_early_stop = False

        self.save_hyperparameters()
        self.context = context
        # model
        self.model = nn.Sequential(
            nn.Conv2d(1, 10, kernel_size=[5, 5]),
            nn.MaxPool2d(2),
            nn.ReLU(),
            nn.Conv2d(10, 20, kernel_size=[5, 5]),
            nn.Dropout2d(),
            nn.MaxPool2d(2),
            nn.ReLU(),
            nn.Flatten(),
            nn.Linear(320, 50),
            nn.ReLU(),
            nn.Linear(50, 10),
            nn.LogSoftmax()
        )

        # loss
        self.loss_fn = nn.NLLLoss()
        self.expected_label_type = np.int64

    def configure_optimizers(self):
        optimizer = optim.Adam(lr=0.001)
        self.context.configure_aggregation_params(optimizer=optimizer)
        return optimizer


def build_trainer(param: HomoNNParam, data, should_label_align=True, trainer=None):
    header = data.schema["header"]
    if trainer is None:
        total_epoch = param.aggregate_every_n_epoch * param.max_iter
        context = PyTorchSAClientContext(
            max_num_aggregation=param.max_iter,
            aggregate_every_n_epoch=param.aggregate_every_n_epoch,
        )
        pl_trainer = pl.Trainer(
            max_epochs=total_epoch,
            min_epochs=total_epoch,
            callbacks=[EarlyStopCallback(context)],
            num_sanity_val_steps=0,
        )
        context.init()
        pl_model = MyFedLightModule(context)
        expected_label_type = pl_model.expected_label_type
        dataset = make_dataset(
            data=data,
            is_train=should_label_align,
            expected_label_type=expected_label_type,
        )

        batch_size = param.batch_size
        if batch_size < 0:
            batch_size = len(dataset)
        dataloader = torch.utils.data.DataLoader(
            dataset=dataset, batch_size=batch_size, num_workers=1
        )
        trainer = MyPyTorchFederatedTrainer(
            pl_trainer=pl_trainer,
            header=header,
            label_mapping=dataset.get_label_align_mapping(),
            pl_model=pl_model,
            context=context,
        )
    else:
        trainer.context.init()
        expected_label_type = trainer.pl_model.expected_label_type

        dataset = make_dataset(
            data=data,
            is_train=should_label_align,
            expected_label_type=expected_label_type,
        )

        batch_size = param.batch_size
        if batch_size < 0:
            batch_size = len(dataset)
        dataloader = torch.utils.data.DataLoader(
            dataset=dataset, batch_size=batch_size, num_workers=1
        )
    return trainer, dataloader


class MyPyTorchFederatedTrainer(PyTorchFederatedTrainer):
    def __init__(
        self,
        pl_trainer: pl.Trainer = None,
        header=None,
        label_mapping=None,
        pl_model: MyFedLightModule = None,
        context: PyTorchSAClientContext = None,
    ):
        self.pl_trainer = pl_trainer
        self.pl_model = pl_model
        self.context = context
        self.header = header
        self.label_mapping = label_mapping

    @classmethod
    def load_model(cls, model_obj, meta_obj, param):

        # restore pl model
        with tempfile.TemporaryDirectory() as d:
            filepath = os.path.join(d, "model.ckpt")
            with open(filepath, "wb") as f:
                f.write(model_obj.saved_model_bytes)
            pl_model = MyFedLightModule.load_from_checkpoint(filepath)

        # restore context
        context = pl_model.context

        # restore pl trainer
        total_epoch = context.max_num_aggregation * context.aggregate_every_n_epoch
        pl_trainer = pl.Trainer(
            max_epochs=total_epoch,
            min_epochs=total_epoch,
            callbacks=[EarlyStopCallback(context)],
            num_sanity_val_steps=0,
        )
        pl_trainer.model = pl_model

        # restore data header
        header = list(model_obj.header)

        # restore label mapping
        label_mapping = {}
        for item in model_obj.label_mapping:
            label = json.loads(item.label)
            mapped = json.loads(item.mapped)
            label_mapping[label] = mapped
        if not label_mapping:
            label_mapping = None

        # restore trainer
        trainer = MyPyTorchFederatedTrainer(
            pl_trainer=pl_trainer,
            header=header,
            label_mapping=label_mapping,
            pl_model=pl_model,
            context=context,
        )

        # restore model param
        param.restore_from_pb(meta_obj.params)
        return trainer
