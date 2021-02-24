#
# Copyright(c) 2021 Intel Corporation
# SPDX-License-Identifier: BSD-3-Clause-Clear
#

import pytest
from storage_devices.lvm import Lvm, LvmConfiguration
from tests.common.lvm_common_utils import prepare_devices, run_fio

from api.cas import casadm
from api.cas.cache import Cache
from api.cas.cache_config import CacheMode
from api.cas.casadm_params import OutputFormat
from api.cas.ioclass_config import IoClass
from core.test_run import TestRun
from storage_devices.disk import DiskType, DiskTypeSet, DiskTypeLowerThan
from test_tools import fs_utils
from test_tools.disk_utils import Filesystem
from test_utils.size import Size, Unit

mountpoint = "/mnt/"
io_target = "/mnt/test"


@pytest.mark.require_disk("cache", DiskTypeSet([DiskType.optane, DiskType.nand]))
@pytest.mark.require_disk("core", DiskTypeLowerThan("cache"))
def test_io_class_lvm_on_cas():
    """
        title: IO class for CAS device behind LVM.
        description: Validate the ability of CAS to cache IO class when CAS device is used as LVM.
        pass_criteria:
          - Create CAS device and LVM on top of it successfully.
          - Loading IO class configuration successfully.
          - Running FIO for file size from IO class from 11 to 21 successfully.
          - Increasing proper statistics as expected.
    """
    with TestRun.step(f"Create CAS device."):

        cache, core = prepare_devices(TestRun.disks['cache'], TestRun.disks['core'],
                                      Size(20, Unit.GibiByte), CacheMode.WB)

    with TestRun.step("Create LVM on CAS device."):
        lvm_filters = ["a/.*/", "r|/dev/sd*|", "r|/dev/hd*|", "r|/dev/xvd*|", "r/disk/", "r/block/",
                       "r|/dev/nvme*|"]
        pv_num = 1
        vg_num = 1
        lv_num = 1
        cache_num = 1
        cas_dev_num = 1

        config = LvmConfiguration(lvm_filters, pv_num, vg_num, lv_num, cache_num, cas_dev_num)

        lvms = Lvm.create_specific_lvm_configuration(core, config)
        lvm = lvms[0]

    with TestRun.step("Create filesystem for LVM and mount it."):
        lvm.create_filesystem(Filesystem.ext4)
        lvm.mount(mountpoint)

    with TestRun.step("Load IO class config."):
        cache.load_io_class("/etc/opencas/ioclass-config.csv")

    with TestRun.step("Run fio for file size from IO class from 11 to 21 "
                      "and check that correct statistics increase."):
        csv = casadm.list_io_classes(cache.cache_id, OutputFormat.csv).stdout
        io_classes = IoClass.csv_to_list(csv)
        file_size = Size(2, Unit.KibiByte)

        for io_class in io_classes:
            if io_class.id < 11 or io_class.id > 21:
                continue

            TestRun.LOGGER.info(f"IO Class ID: {io_class.id}, class name: {io_class.rule}")
            cache.reset_counters()

            TestRun.LOGGER.info(f"Running FIO with verification on LVM "
                                f"[IO class ID {io_class.id}].")
            run_fio(file_size, io_target)

            TestRun.LOGGER.info(f"Checking statistics [IO class ID {io_class.id}].")
            check_statistics(cache, io_classes, io_class.id)

            if file_size < Size(256, Unit.MebiByte):
                file_size *= 4
            else:
                file_size = 1100 * Size(1, Unit.MebiByte)

            fs_utils.remove(io_target)

    with TestRun.step("Remove LVMs."):
        TestRun.executor.run(f"umount {mountpoint}")
        Lvm.remove_all()


def check_statistics(cache: Cache, io_classes: [], tested_class: int):
    write_policy = cache.get_cache_mode()
    stat_to_check = "Total requests"

    for io_class in io_classes:
        class_stats = cache.get_io_class_statistics(io_class.id)

        stat_value = class_stats.request_stats.requests_total

        if "metadata" in io_class.rule or "misc" in io_class.rule:
            TestRun.LOGGER.info("Check skipped for tested IO Class for metadata and misc class")

            TestRun.LOGGER.info(f"[{write_policy}] {stat_to_check} for IO Class {io_class.id} "
                                f"[{io_class.rule}]: {stat_value}")
            continue

        if io_class.id == tested_class:
            if stat_value == 0:
                TestRun.LOGGER.error(f"[{write_policy}] {stat_to_check} too low for IO Class "
                                     f"{io_class.id} [{io_class.rule}]: {stat_value}")
                TestRun.executor.run(f"ls -la {mountpoint}*")
            else:
                TestRun.LOGGER.info(f"[{write_policy}] {stat_to_check} for IO Class {io_class.id} "
                                    f"[{io_class.rule}]: {stat_value}")
                continue

        if stat_value > 0:
            TestRun.LOGGER.error(f"[{write_policy}] {stat_to_check} too high for IO Class "
                                 f"{io_class.id} [{io_class.rule}]: {stat_value}")
            TestRun.executor.run(f"ls -la {mountpoint}*")
        else:
            TestRun.LOGGER.info(f"[{write_policy}] {stat_to_check} for IO Class {io_class.id} "
                                f"[{io_class.rule}]: {stat_value}")
