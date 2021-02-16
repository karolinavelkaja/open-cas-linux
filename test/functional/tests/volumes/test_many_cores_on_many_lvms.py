#
# Copyright(c) 2021 Intel Corporation
# SPDX-License-Identifier: BSD-3-Clause-Clear
#

import pytest
from storage_devices.lvm import Lvm, LvmConfiguration
from tests.common.lvm_common_utils import run_fio_on_lvm, get_test_configuration

from api.cas import casadm
from core.test_run import TestRun
from storage_devices.disk import DiskType, DiskTypeSet, DiskTypeLowerThan
from test_utils.size import Size, Unit


@pytest.mark.require_disk("cache", DiskTypeSet([DiskType.optane, DiskType.nand]))
@pytest.mark.require_disk("core", DiskTypeLowerThan("cache"))
def test_many_cores_on_many_lvms():
    """
        title: Test for CAS creation with lvms as cores: 1 cache, 16 lvms, 16 cores.
        description: |
          Validation of LVM support, CAS with 1 cache and 16 lvms as 16 cores.
        pass_criteria:
          - LVMs created successfully.
          - CAS devices created successfully.
          - FIO with verification ran successfully.
          - Configuration after reboot match configuration before.
    """
    with TestRun.step(f"Prepare devices."):
        cache_device = TestRun.disks['cache']
        core_device = TestRun.disks['core']

        cache_device.create_partitions([Size(10, Unit.GibiByte)])
        core_device.create_partitions([Size(10, Unit.GibiByte)])

        cache_dev = cache_device.partitions[0]
        core_dev = core_device.partitions

    with TestRun.step("Create LVMs."):
        lvm_filters = []
        pv_num = 1
        vg_num = 1
        lv_num = 16
        cache_num = 1
        cas_dev_num = 16

        config = LvmConfiguration(lvm_filters, pv_num, vg_num, lv_num, cache_num, cas_dev_num)

        lvms = Lvm.create_specific_lvm_configuration(core_dev, config, lvm_as_core=True)

    with TestRun.step(f"Create CAS device."):
        cache = casadm.start_cache(cache_dev, force=True)
        cores = []
        for lvm in lvms:
            cores.append(cache.add_core(lvm))

    with TestRun.step("Run FIO with verification on LVM."):
        run_fio_on_lvm(cores)

    with TestRun.step("Flush buffers"):
        for core in cores:
            TestRun.executor.run(f"hdparm -f {core.path}")

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
        run_fio_on_lvm(cores)

    with TestRun.step("Remove CAS devices."):
        casadm.remove_all_detached_cores()
        casadm.stop_all_caches()

    with TestRun.step("Remove LVMs."):
        Lvm.remove_all()
