#
# Copyright(c) 2021 Intel Corporation
# SPDX-License-Identifier: BSD-3-Clause-Clear
#
import time
from datetime import timedelta

import pytest

from api.cas import casadm, cli_messages, cli
from api.cas.cache_config import CacheMode, CleaningPolicy
from core.test_run import TestRun
from storage_devices.disk import DiskTypeSet, DiskType, DiskTypeLowerThan
from test_tools.disk_utils import get_device_filesystem_type, Filesystem
from test_tools.fio.fio import Fio
from test_tools.fio.fio_param import IoEngine, ReadWrite
from test_utils.disk_finder import get_system_disks
from test_utils.output import CmdException
from test_utils.size import Size, Unit


@pytest.mark.require_disk("cache", DiskTypeSet([DiskType.optane, DiskType.nand]))
@pytest.mark.require_disk("core", DiskTypeLowerThan("cache"))
def test_zero_metadata_negative_cases():
    """
        title: Test for '--zero-metadata' negative cases.
        description: |
          Test for '--zero-metadata' scenarios with expected failures.
        pass_criteria:
          - Zero metadata without '-force' failed when run on cache.
          - Zero metadata failed when run on system drive.
          - After zero metadata (success) on stopped cache load cache failed.
    """
    with TestRun.step("Prepare cache and core devices."):
        cache_dev, core_dev = prepare_devices()

    with TestRun.step("Start cache."):
        cache = casadm.start_cache(cache_dev, CacheMode.WT, force=True)

    with TestRun.step("Try to zero metadata and validate error message."):
        output = TestRun.executor.run(cli.zero_metadata_cmd(str(cache_dev.path)))
        if output.exit_code != 0:
            cli_messages.check_stderr_msg(output, cli_messages.unavailable_device)
        else:
            TestRun.LOGGER.error("Zeroing metadata should fail!")

    with TestRun.step("Try to zero metadata on system disk."):
        os_disks = get_system_disks()
        for os_disk in os_disks:
            output = TestRun.executor.run(cli.zero_metadata_cmd(str(os_disk)))
            if output.exit_code != 0:
                cli_messages.check_stderr_msg(output, cli_messages.error_handling)
            else:
                TestRun.LOGGER.error("Zeroing metadata should fail!")

    with TestRun.step("Stop cache."):
        casadm.stop_all_caches()

    with TestRun.step("Run zero metadata."):
        output = TestRun.executor.run(cli.zero_metadata_cmd(str(cache_dev.path)))
        if output.exit_code == 0:
            TestRun.LOGGER.info("Zeroing metadata ran successfully!")
        else:
            TestRun.LOGGER.error("Zero metadata should works for cache device after cache stop!")

    with TestRun.step("Load cache."):
        try:
            cache = casadm.load_cache(cache_dev)
        except CmdException:
            TestRun.LOGGER.info("Load cache failed as expected.")


@pytest.mark.require_disk("cache", DiskTypeSet([DiskType.optane, DiskType.nand]))
@pytest.mark.require_disk("core", DiskTypeLowerThan("cache"))
@pytest.mark.parametrizex("filesystem", Filesystem)
def test_zero_metadata_filesystem(filesystem):
    """
        title: Test Test for '--zero-metadata' and filesystem.
        description: |
          Test for '--zero-metadata' on drive with filesystem.
        pass_criteria:
          - Zero metadata on device with filesystem failed and not removed filesystem.
          - Zero metadata on mounted device failed.
    """
    mount_point = "/mnt"
    with TestRun.step("Prepare devices."):
        cache_disk, core_disk = prepare_devices()

    with TestRun.step("Start cache and add core."):
        cache = casadm.start_cache(cache_disk, force=True)
        core = cache.add_core(core_disk)

    with TestRun.step("Create filesystem on core device."):
        core.create_filesystem(filesystem)

    with TestRun.step("Zero metadata on core device and check if filesystem still exists"):
        output = TestRun.executor.run(cli.zero_metadata_cmd(str(core.path)))
        if output.exit_code != 0:
            cli_messages.check_stderr_msg(output, cli_messages.no_cas_metadata)
        else:
            TestRun.LOGGER.error("Zeroing metadata should fail!")

        file_system = get_device_filesystem_type(core.get_device_id())

        if file_system != filesystem:
            TestRun.LOGGER.error(f"After zero metadata on core - filesystem ({file_system}) "
                                 f"not as expected ({filesystem})!")

    with TestRun.step("Mount core."):
        core.mount(mount_point)

    with TestRun.step("Zero metadata on mounted core device and validate result"):
        output = TestRun.executor.run(cli.zero_metadata_cmd(str(core.path)))
        if output.exit_code != 0:
            cli_messages.check_stderr_msg(output, cli_messages.unavailable_device)
        else:
            TestRun.LOGGER.error("Zeroing metadata should fail!")


@pytest.mark.require_disk("cache", DiskTypeSet([DiskType.optane, DiskType.nand]))
@pytest.mark.require_disk("core", DiskTypeLowerThan("cache"))
def test_zero_metadata_dirty_data():
    """
        title: Test for '--zero-metadata' and dirty data scenario.
        description: |
          Test for '--zero-metadata' with&without 'force' option if there are dirty data on cache.
        pass_criteria:
          - Zero metadata without force failed on cache with dirty data.
          - Zero metadata with force ran successfully on cache with dirty data.
          - Cache started successfully after operations.
    """
    with TestRun.step("Prepare cache and core devices."):
        with TestRun.step("Prepare devices."):
            cache_disk, core_disk = prepare_devices()

    with TestRun.step("Start cache."):
        cache = casadm.start_cache(cache_disk, CacheMode.WB, force=True)
        core = cache.add_core(core_disk)
        cache.set_cleaning_policy(CleaningPolicy.nop)

    with TestRun.step("Run workload on CAS"):
        fio_run_fill = Fio().create_command()
        fio_run_fill.io_engine(IoEngine.libaio)
        fio_run_fill.direct()
        fio_run_fill.read_write(ReadWrite.randwrite)
        fio_run_fill.io_depth(16)
        fio_run_fill.block_size(Size(1, Unit.MebiByte))
        fio_run_fill.target(core.path)
        fio_run_fill.run_time(timedelta(seconds=5))
        fio_run_fill.time_based()
        fio_run_fill.run()

    with TestRun.step("Stop cache without flushing dirty data."):
        cache.stop(no_data_flush=True)

    with TestRun.step("Start cache (expect to fail)."):
        try:
            cache = casadm.start_cache(cache_disk, CacheMode.WB)
        except CmdException:
            TestRun.LOGGER.info("Start cache failed as expected.")

    with TestRun.step("Zero metadata on CAS device without force"):
        output = TestRun.executor.run(cli.zero_metadata_cmd(str(cache_disk.path)))
        if output.exit_code != 0:
            cli_messages.check_stderr_msg(output, cli_messages.cache_dirty_data)
        else:
            TestRun.LOGGER.error("Zeroing metadata without force should fail!")

    with TestRun.step("Zero metadata on cache device with force"):
        output = TestRun.executor.run(cli.zero_metadata_cmd(str(cache_disk.path), force=True))
        if output.exit_code == 0:
            TestRun.LOGGER.info("Zeroing metadata with force ran successfully!")
        else:
            TestRun.LOGGER.error("Zero metadata with force should works for cache device!")

        with TestRun.step("Start cache."):
            try:
                cache = casadm.start_cache(cache_disk, CacheMode.WB)
                TestRun.LOGGER.info("Cache started successfully.")
            except CmdException:
                TestRun.LOGGER.error("Start cache failed.")


@pytest.mark.require_disk("cache", DiskTypeSet([DiskType.optane, DiskType.nand]))
@pytest.mark.require_disk("core", DiskTypeLowerThan("cache"))
def test_zero_metadata_dirty_shutdown():
    """
        title: Test for '--zero-metadata' and dirty shutdown scenario.
        description: |
          Test for '--zero-metadata' with and without 'force' option on cache which had been dirty
          shut down before.
        pass_criteria:
          - Zero metadata without force failed on cache after dirty shutdown.
          - Zero metadata with force ran successfully on cache after dirty shutdown.
          - Cache started successfully after operations.
    """
    with TestRun.step("Prepare cache and core devices."):
        with TestRun.step("Prepare devices."):
            cache_disk = TestRun.disks['cache']
            cache_disk.create_partitions([Size(2, Unit.GibiByte)])
            cache_dev = cache_disk.partitions[0]
            core_disk = TestRun.disks['core']
            core_disk.create_partitions([Size(5, Unit.GibiByte)])

    with TestRun.step("Start cache."):
        cache = casadm.start_cache(cache_dev, CacheMode.WT, force=True)
        core = cache.add_core(core_disk)

    with TestRun.step("Unplug cache device."):
        cache_disk.unplug()

    with TestRun.step("Stop cache without flush."):
        try:
            cache.stop(no_data_flush=True)
        except CmdException:
            TestRun.LOGGER.info("This could ended with error (expected)")

    with TestRun.step("Plug cache device."):
        cache_disk.plug()
        time.sleep(1)

    with TestRun.step("Start cache (expect to fail)."):
        try:
            cache = casadm.start_cache(cache_dev, CacheMode.WT)
            TestRun.LOGGER.error("Starting cache should fail!")
        except CmdException:
            TestRun.LOGGER.info("Start cache failed as expected.")

    with TestRun.step("Zero metadata on CAS device without force"):
        output = TestRun.executor.run(cli.zero_metadata_cmd(str(cache_dev.path)))
        if output.exit_code != 0:
            cli_messages.check_stderr_msg(output, cli_messages.cache_dirty_shutdown)
        else:
            TestRun.LOGGER.error("Zeroing metadata without force should fail!")

    with TestRun.step("Zero metadata on cache device with force"):
        output = TestRun.executor.run(cli.zero_metadata_cmd(str(cache_dev.path), force=True))
        if output.exit_code == 0:
            TestRun.LOGGER.info("Zeroing metadata with force ran successfully!")
        else:
            TestRun.LOGGER.error("Zero metadata with force should works for cache device!")

    with TestRun.step("Start cache."):
        try:
            cache = casadm.start_cache(cache_dev, CacheMode.WT)
            TestRun.LOGGER.info("Cache started successfully.")
        except CmdException:
            TestRun.LOGGER.error("Start cache failed.")


def prepare_devices():
    cache_disk = TestRun.disks['cache']
    cache_disk.create_partitions([Size(2, Unit.GibiByte)])
    cache_part = cache_disk.partitions[0]
    core_disk = TestRun.disks['core']
    core_disk.create_partitions([Size(5, Unit.GibiByte)])

    return cache_part, core_disk
