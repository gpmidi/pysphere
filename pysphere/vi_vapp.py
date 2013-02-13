# --
# Copyright (c) 2012, Sebastian Tello
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#   * Neither the name of copyright holders nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# --

import sys
import time
import os

from pysphere.resources import VimService_services as VI
from pysphere import VIException, VIApiException, FaultTypes
from pysphere import VITask, VIProperty, VIMor, MORTypes
from pysphere.vi_snapshot import VISnapshot

class VIVApp:
    def __init__(self, server, mor):
        self._server = server
        self._mor = mor
        self._resource_pool = None
        self.properties = None
        self._properties = {}
        self.__update_properties()
#        try:
#            self.__update_properties()
#        except Exception, e:
#            print repr(e)


        try:
            guest_op = VIProperty(self._server, self._server._do_service_content
                                                        .GuestOperationsManager)
            self._auth_mgr = guest_op.authManager._obj
            try:
                self._file_mgr = guest_op.fileManager._obj
            except AttributeError:
                # file manager not present
                pass
            try:
                # process manager not present
                self._proc_mgr = guest_op.processManager._obj
            except:
                pass
        except AttributeError:
            # guest operations not supported (since API 5.0)
            pass

    # ------------------- #
    # -- POWER METHODS -- #
    # ------------------- #
    def power_on(self, sync_run=True):
        """Attempts to power on the vApp. If @sync_run is True (default) waits for
        the task to finish, and returns (raises an exception if the task didn't
        succeed). If sync_run is set to False the task is started an a VITask
        instance is returned. """
        try:
            request = VI.PowerOnVApp_TaskRequestMsg
            mor_vapp = request.new__this(self._mor)
            mor_vapp.set_attribute_type(self._mor.get_attribute_type())
            request.set_element__this(mor_vapp)

            task = self._server._proxy.PowerOnVApp_Task(request)._returnval
            vi_task = VITask(task, self._server)
            if sync_run:
                status = vi_task.wait_for_state([vi_task.STATE_SUCCESS,
                                                 vi_task.STATE_ERROR])
                if status == vi_task.STATE_ERROR:
                    raise VIException(vi_task.get_error_message(),
                                      FaultTypes.TASK_ERROR)
                return

            return vi_task

        except (VI.ZSI.FaultException), e:
            raise VIApiException(e)

    def power_off(self, sync_run=True):
        """Attemps to power off the vApp. If @sync_run is True (default) waits for
        the task to finish, and returns (raises an exception if the task didn't
        succeed). If sync_run is set to False the task is started an a VITask
        instance is returned. """
        try:
            request = VI.PowerOffVApp_TaskRequestMsg()
            mor_vm = request.new__this(self._mor)
            mor_vm.set_attribute_type(self._mor.get_attribute_type())
            request.set_element__this(mor_vm)

            task = self._server._proxy.PowerOffVApp_Task(request)._returnval
            vi_task = VITask(task, self._server)
            if sync_run:
                status = vi_task.wait_for_state([vi_task.STATE_SUCCESS,
                                                 vi_task.STATE_ERROR])
                if status == vi_task.STATE_ERROR:
                    raise VIException(vi_task.get_error_message(),
                                      FaultTypes.TASK_ERROR)
                return

            return vi_task

        except (VI.ZSI.FaultException), e:
            raise VIApiException(e)

    def get_status(self, basic_status=False):
        """Returns any of the status strings defined in VMPowerState:
        basic statuses:
            'POWERED ON', 'POWERED OFF', 'SUSPENDED', 'BLOCKED ON MSG'
        extended_statuses (only available for vCenter):
            'POWERING ON', 'POWERING OFF', 'SUSPENDING', 'RESETTING', 
            'REVERTING TO SNAPSHOT', 'UNKNOWN'
        if basic_status is False (defautl) and the server is a vCenter, then
        one of the extended statuses might be returned.
        """
        # we can't check tasks in a VMWare Server or ESXi
        if not basic_status and self._server.get_api_type() == 'VirtualCenter':
            try:
                if not self._mor_vm_task_collector:
                    self.__create_pendant_task_collector()
            except VIApiException:
                basic_status = True

        # get the VM current power state, and messages blocking if any
        vi_power_states = {'poweredOn':VMPowerState.POWERED_ON,
                           'poweredOff': VMPowerState.POWERED_OFF,
                           'suspended': VMPowerState.SUSPENDED}

        power_state = None

        oc_vm_status_msg = self._server._get_object_properties(
                      self._mor,
                      property_names=['runtime.question', 'runtime.powerState']
                      )
        properties = oc_vm_status_msg.PropSet
        for prop in properties:
            if prop.Name == 'runtime.powerState':
                power_state = prop.Val
            if prop.Name == 'runtime.question':
                return VMPowerState.BLOCKED_ON_MSG

        # we can't check tasks in a VMWare Server
        if self._server.get_api_type() != 'VirtualCenter' or basic_status:
            return vi_power_states.get(power_state, VMPowerState.UNKNOWN)

        # on the other hand, get the current task running or queued for this VM
        oc_task_history = self._server._get_object_properties(
                      self._mor_vm_task_collector,
                      property_names=['latestPage']
                      )
        properties = oc_task_history.PropSet
        if len(properties) == 0:
            return vi_power_states.get(power_state, VMPowerState.UNKNOWN)
        for prop in properties:
            if prop.Name == 'latestPage':
                tasks_info_array = prop.Val.TaskInfo
                if len(tasks_info_array) == 0:
                    return vi_power_states.get(power_state, VMPowerState.UNKNOWN)
                for task_info in tasks_info_array:
                    desc = task_info.DescriptionId
                    if task_info.State in ['success', 'error']:
                        continue

                    if desc == 'VirtualMachine.powerOff' and power_state in [
                                                      'poweredOn', 'suspended']:
                        return VMPowerState.POWERING_OFF
                    if desc in ['VirtualMachine.revertToCurrentSnapshot',
                                'vm.Snapshot.revert']:
                        return VMPowerState.REVERTING_TO_SNAPSHOT
                    if desc == 'VirtualMachine.reset' and power_state in [
                                                      'poweredOn', 'suspended']:
                        return VMPowerState.RESETTING
                    if desc == 'VirtualMachine.suspend' and power_state in [
                                                                   'poweredOn']:
                        return VMPowerState.SUSPENDING
                    if desc in ['Drm.ExecuteVmPowerOnLRO',
                                'VirtualMachine.powerOn'] and power_state in [
                                                     'poweredOff', 'suspended']:
                        return VMPowerState.POWERING_ON
                return vi_power_states.get(power_state, VMPowerState.UNKNOWN)

    def is_powering_off(self):
        """Returns True if the VM is being powered off"""
        return self.get_status() == VMPowerState.POWERING_OFF

    def is_powered_off(self):
        """Returns True if the VM is powered off"""
        return self.get_status() == VMPowerState.POWERED_OFF

    def is_powering_on(self):
        """Returns True if the VM is being powered on"""
        return self.get_status() == VMPowerState.POWERING_ON

    def is_powered_on(self):
        """Returns True if the VM is powered off"""
        return self.get_status() == VMPowerState.POWERED_ON

    def is_suspending(self):
        """Returns True if the VM is being suspended"""
        return self.get_status() == VMPowerState.SUSPENDING

    def is_suspended(self):
        """Returns True if the VM is suspended"""
        return self.get_status() == VMPowerState.SUSPENDED

    def is_resetting(self):
        """Returns True if the VM is being resetted"""
        return self.get_status() == VMPowerState.RESETTING

    def is_blocked_on_msg(self):
        """Returns True if the VM is blocked because of a question message"""
        return self.get_status() == VMPowerState.BLOCKED_ON_MSG

    def is_reverting(self):
        """Returns True if the VM is being reverted to a snapshot"""
        return self.get_status() == VMPowerState.REVERTING_TO_SNAPSHOT

    #-------------------#
    # -- OTHER METHODS --#
    #-------------------#

    def get_property(self, name='', from_cache=True):
        """"Returns the VM property with the given @name or None if the property
        doesn't exist or have not been set. The property is looked in the cached
        info obtained from the last time the server was requested.
        If you expect to get a volatile property (that might have changed since
        the last time the properties were queried), you may set @from_cache to
        True to refresh all the properties.
        The properties you may get are:
            name: Name of this entity, unique relative to its parent.
            path: Path name to the configuration file for the virtual machine
                  e.g., the .vmx file.
            guest_id:
            guest_full_name:
            hostname:
            ip_address:
            mac_address
            net: [{connected, mac_address, ip_addresses, network},...]
        """
        if not from_cache:
            self.__update_properties()
        return self._properties.get(name)

    def get_properties(self, from_cache=True):
        """Returns a dictionary of property of this VM.
        If you expect to get a volatile property (that might have changed since
        the last time the properties were queried), you may set @from_cache to
        True to refresh all the properties before retrieve them."""
        if not from_cache:
            self.__update_properties()
        return self._properties.copy()


    def get_resource_pool_name(self):
        """Returns the name of the resource pool where this VM belongs to. Or
        None if there isn't any or it can't be retrieved"""
        if self._resource_pool:
            oc = self._server._get_object_properties(
                                   self._resource_pool, property_names=['name'])
            if not hasattr(oc, 'PropSet'):
                return None
            prop_set = oc.PropSet
            if len(prop_set) == 0:
                return None
            for prop in prop_set:
                if prop.Name == 'name':
                    return prop.Val


    def set_extra_config(self, settings, sync_run=True):
        """Sets the advanced configuration settings (as the ones on the .vmx
        file).
          * settings: a key-value pair dictionary with the settings to 
            set/change
          * sync_run: if True (default) waits for the task to finish and returns
            (raises an exception if the task didn't succeed). If False the task 
            is started and a VITask instance is returned
        E.g.:
            #prevent virtual disk shrinking
            vm.set_extra_config({'isolation.tools.diskWiper.disable':'TRUE',
                                 'isolation.tools.diskShrink.disable':'TRUE'})
        """
        try:
            request = VI.ReconfigVM_TaskRequestMsg()
            _this = request.new__this(self._mor)
            _this.set_attribute_type(self._mor.get_attribute_type())
            request.set_element__this(_this)

            spec = request.new_spec()
            extra_config = []
            for k, v in settings.iteritems():
                ec = spec.new_extraConfig()
                ec.set_element_key(str(k))
                ec.set_element_value(str(v))
                extra_config.append(ec)
            spec.set_element_extraConfig(extra_config)

            request.set_element_spec(spec)
            task = self._server._proxy.ReconfigVM_Task(request)._returnval
            vi_task = VITask(task, self._server)
            if sync_run:
                status = vi_task.wait_for_state([vi_task.STATE_SUCCESS,
                                                 vi_task.STATE_ERROR])
                if status == vi_task.STATE_ERROR:
                    raise VIException(vi_task.get_error_message(),
                                      FaultTypes.TASK_ERROR)
                return
            return vi_task
        except (VI.ZSI.FaultException), e:
            raise VIApiException(e)

    #---------------------#
    # -- PRIVATE METHODS --#
    #---------------------#

    def __create_pendant_task_collector(self):
        """sets the MOR of a TaskHistoryCollector which will retrieve
        the lasts task info related to this VM (or any suboject as snapshots)
        for those task in 'running' or 'queued' states"""
        try:
            mor_task_manager = self._server._do_service_content.TaskManager

            request = VI.CreateCollectorForTasksRequestMsg()
            mor_tm = request.new__this(mor_task_manager)
            mor_tm.set_attribute_type(mor_task_manager.get_attribute_type())
            request.set_element__this(mor_tm)

            do_task_filter_spec = request.new_filter()
            do_task_filter_spec.set_element_state(['running', 'queued'])

            do_tfs_by_entity = do_task_filter_spec.new_entity()
            mor_entity = do_tfs_by_entity.new_entity(self._mor)
            mor_entity.set_attribute_type(self._mor.get_attribute_type())
            do_tfs_by_entity.set_element_entity(mor_entity)
            do_tfs_by_entity.set_element_recursion('all')

            do_task_filter_spec.set_element_entity(do_tfs_by_entity)

            request.set_element_filter(do_task_filter_spec)
            ret = self._server._proxy.CreateCollectorForTasks(request) \
                                                                     ._returnval
            self._mor_vm_task_collector = ret

        except (VI.ZSI.FaultException), e:
            raise VIApiException(e)

    def __validate_authentication(self, auth_obj):
        if not self._auth_mgr:
            raise VIException("Guest Operations only available since API 5.0",
                              FaultTypes.NOT_SUPPORTED)
        try:
            request = VI.ValidateCredentialsInGuestRequestMsg()
            _this = request.new__this(self._auth_mgr)
            _this.set_attribute_type(self._auth_mgr.get_attribute_type())
            request.set_element__this(_this)
            vm = request.new_vm(self._mor)
            vm.set_attribute_type(self._mor.get_attribute_type())
            request.set_element_vm(vm)
            request.set_element_auth(auth_obj)
            self._server._proxy.ValidateCredentialsInGuest(request)
        except (VI.ZSI.FaultException), e:
            raise VIApiException(e)

    def __update_properties(self):
        """Refreshes the properties retrieved from the virtual machine
        (i.e. name, path, snapshot tree, etc). To reduce traffic, all the
        properties are retrieved from one shot, if you expect changes, then you
        should call this method before other"""

        try:
            self.properties = VIProperty(self._server, self._mor)
        except (VI.ZSI.FaultException), e:
            raise VIApiException(e)

        p = {}
        p['name'] = self.properties.name

        # Copy major properties when possible
        for k, v in self.properties._values.items():
            if hasattr(self.properties, k):
                attr = getattr(self.properties, k)
                if hasattr(attr, '_values'):
                    attr._get_all()
                    p[k] = {}
                    for subk, subv in attr._values.items():
                        p[k][subk] = subv
                else:
                    p[k] = v

        # Basic vapp config info
        if hasattr(self.properties, "vAppConfig"):
            p['annotation'] = self.properties.vAppConfig.annotation
            p['installBootRequired'] = self.properties.vAppConfig.installBootRequired
            p['installBootStopDelay'] = self.properties.vAppConfig.installBootStopDelay
            p['instanceUuid'] = self.properties.vAppConfig.instanceUuid
            p['ipAssignment'] = self.properties.vAppConfig.ipAssignment
            p['product'] = self.properties.vAppConfig.product
            p['entities'] = []
            if hasattr(self.properties.vAppConfig, 'entityConfig'):
                for ent in self.properties.vAppConfig.entityConfig:
                    p['entities'].append(vAppEntityConfig(ent))

        self._properties = p


class vAppEntityConfig(object):
    def __init__(self, entityConfig):
        entityConfig._get_all()
        for name, value in entityConfig._values.items():
            setattr(self, name, value)

    def __repr__(self):
        return "<vAppEntityConfig Tag=%r>" % self.tag

    def __str__(self):
        return "vAppEntity %s" % self.tag


class VMPowerState:
    POWERED_ON = 'POWERED ON'
    POWERED_OFF = 'POWERED OFF'
    SUSPENDED = 'SUSPENDED'
    POWERING_ON = 'POWERING ON'
    POWERING_OFF = 'POWERING OFF'
    SUSPENDING = 'SUSPENDING'
    RESETTING = 'RESETTING'
    BLOCKED_ON_MSG = 'BLOCKED ON MSG'
    REVERTING_TO_SNAPSHOT = 'REVERTING TO SNAPSHOT'
    UNKNOWN = 'UNKNOWN'
