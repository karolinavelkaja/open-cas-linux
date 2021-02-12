#
# Copyright(c) 2021 Intel Corporation
# SPDX-License-Identifier: BSD-3-Clause-Clear
#

import pytest
from storage_devices.lvm import Lvm, LvmConfiguration
from tests.common.lvm_common_utils import get_test_configuration, run_fio_on_lvm

from api.cas import casadm
from api.cas.cache_config import CacheMode
from core.test_run import TestRun
from storage_devices.disk import DiskType, DiskTypeSet, DiskTypeLowerThan
from test_utils.size import Size, Unit

lv_count = 16


@pytest.mark.require_disk("cache", DiskTypeSet([DiskType.optane, DiskType.nand]))
@pytest.mark.require_disk("core", DiskTypeLowerThan("cache"))
def test_many_lvms_on_single_core():
    """
        title: Test for LVM creation on CAS device - many lvms on single core.
        description: |
          Validation of LVM support, many LVMs (16) created on CAS device (1 cache, 1 core).
        pass_criteria:
          - CAS devices created successfully.
          - LVMs created successfully.
          - FIO with verification ran successfully.
          - Configuration after reboot match configuration before.
    """
    with TestRun.step(f"Create CAS device."):

        cache, core = prepare_devices(TestRun.disks['cache'], TestRun.disks['core'],
                                      Size(8, Unit.GibiByte))

    with TestRun.step("Create LVMs on CAS device."):
        lvm_filters = ["a/.*/", "r|/dev/sd*|", "r|/dev/hd*|", "r|/dev/xvd*|", "r/disk/", "r/block/",
                       "r|/dev/nvme*|"]
        pv_num = 1
        vg_num = 1
        lv_num = 16
        cache_num = 1
        cas_dev_num = 1

        config = LvmConfiguration(lvm_filters, pv_num, vg_num, lv_num, cache_num, cas_dev_num)

        lvms = Lvm.create_specific_lvm_configuration(core, config)

    with TestRun.step("Run FIO with verification on LVM."):
        run_fio_on_lvm(lvms)

    with TestRun.step("Flush buffers"):
        for lvm in lvms:
            TestRun.executor.run(f"hdparm -f {lvm.path}")

    with TestRun.step("Create init config from running configuration"):
        config_before_reboot, devices_before = get_test_configuration()

    with TestRun.step("Reboot system."):
        TestRun.executor.reboot()

    with TestRun.step("Validate running configuration"):
        config_after_reboot, devices_after = get_test_configuration()

        if config_after_reboot == config_before_reboot and devices_after == devices_before:
            TestRun.LOGGER.info(f"Configuration is as expected")
        else:
            TestRun.LOGGER.error(f"Configuration changed after reboot.")

    with TestRun.step("Run FIO with verification on LVM."):
        run_fio_on_lvm(lvms)

    with TestRun.step("Remove LVMs."):
        Lvm.remove_all()


def prepare_devices(cache_dev, core_dev, partitions_size: Size, cache_mode: CacheMode = None):
    cache_dev.create_partitions([partitions_size])
    core_dev.create_partitions([partitions_size])

    cache = casadm.start_cache(cache_dev.partitions[0], cache_mode=cache_mode, force=True)
    core = cache.add_core(core_dev.partitions[0])

    return cache, core
