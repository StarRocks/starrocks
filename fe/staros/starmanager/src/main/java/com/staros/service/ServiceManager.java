// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.staros.service;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import com.staros.exception.ExceptionCode;
import com.staros.exception.InvalidArgumentStarException;
import com.staros.exception.NotExistStarException;
import com.staros.exception.StarException;
import com.staros.filestore.FileStore;
import com.staros.filestore.FileStoreMgr;
import com.staros.journal.DummyJournalSystem;
import com.staros.journal.Journal;
import com.staros.journal.JournalSystem;
import com.staros.journal.StarMgrJournal;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.SectionType;
import com.staros.proto.ServiceInfo;
import com.staros.proto.ServiceManagerFileStoreMgrHeader;
import com.staros.proto.ServiceManagerImageData;
import com.staros.proto.ServiceManagerImageMetaFooter;
import com.staros.proto.ServiceManagerImageMetaHeader;
import com.staros.proto.ServiceManagerShardManagerHeader;
import com.staros.proto.ServiceState;
import com.staros.proto.ServiceTemplateInfo;
import com.staros.schedule.Scheduler;
import com.staros.section.Section;
import com.staros.section.SectionReader;
import com.staros.section.SectionWriter;
import com.staros.shard.ShardManager;
import com.staros.util.Constant;
import com.staros.util.IdGenerator;
import com.staros.util.LockCloseable;
import com.staros.util.LogUtils;
import com.staros.util.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.staros.util.Utils.executeNoExceptionOrDie;

/**
 * Service Manager handles all operations that are related to Service, including
 * service registration, service bootstrap, etc.
 */
public class ServiceManager {
    private static final Logger LOG = LogManager.getLogger(ServiceManager.class);

    private final Map<String, ServiceTemplate> serviceTemplates;
    private final Map<String, Service> services;
    private final ReentrantReadWriteLock lock;

    private final JournalSystem journalSystem;
    private final IdGenerator idGenerator;

    private Scheduler shardScheduler;

    // FOR TEST
    public ServiceManager() {
        this(new DummyJournalSystem(), new IdGenerator(null));
    }

    public ServiceManager(JournalSystem journalSystem, IdGenerator idGenerator) {
        this.serviceTemplates = new HashMap<>();
        this.services = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();

        this.journalSystem = journalSystem;
        this.idGenerator = idGenerator;
    }

    /**
     * Register a service template
     *
     * @param name
     *            service template name
     * @param serviceComponents
     *            service component that describes this service template
     *
     * @throws StarException
     *             if service template already exists
     */
    public void registerService(String name, List<String> serviceComponents) throws StarException {
        try (LockCloseable lock = new LockCloseable(writeLock())) {
            if (serviceTemplates.containsKey(name)) {
                throw new StarException(ExceptionCode.ALREADY_EXIST,
                        String.format("service template %s already exist.", name));
            }

            ServiceTemplate serviceTemplate = new ServiceTemplate(name, serviceComponents);

            Journal journal = StarMgrJournal.logRegisterService(serviceTemplate);
            journalSystem.write(journal);

            executeNoExceptionOrDie(() -> serviceTemplates.put(name, serviceTemplate));

            LOG.info("service {} registered.", name);
        }
    }

    /**
     * Deregister a service template
     *
     * @param name
     *            service template name
     *
     * @throws StarException
     *             if service template does not exist, or
     *             this service template has running service
     */
    public void deregisterService(String name) throws StarException {
        try (LockCloseable lock = new LockCloseable(writeLock())) {
            ServiceTemplate serviceTemplate = serviceTemplates.get(name);
            if (serviceTemplate == null) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service template %s not exist.", name));
            }

            for (Service service : services.values()) {
                if (service.getServiceTemplateName().equals(name)) {
                    throw new StarException(ExceptionCode.NOT_ALLOWED,
                            String.format("service template %s has running service.", name));
                }
            }

            Journal journal = StarMgrJournal.logDeregisterService(name);
            journalSystem.write(journal);

            executeNoExceptionOrDie(() -> serviceTemplates.remove(name));

            LOG.info("service {} deregistered.", name);
        }
    }

    /**
     * Bootstrap a service
     *
     * @param serviceTemplateName
     *            service template name
     * @param serviceName
     *            service name
     *
     * @throws StarException
     *             if service template does not exist, or
     *             this service is already bootstraped
     */
    public String bootstrapService(String serviceTemplateName, String serviceName) throws StarException {
        try (LockCloseable lock = new LockCloseable(writeLock())) {
            ServiceTemplate serviceTemplate = serviceTemplates.get(serviceTemplateName);
            if (serviceTemplate == null) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service template %s not exist.", serviceTemplateName));
            }

            for (Service service : services.values()) {
                if (service.getServiceName().equals(serviceName)) {
                    throw new StarException(ExceptionCode.ALREADY_EXIST,
                            String.format("service %s already exist.", serviceName));
                }
            }

            String serviceId = UUID.randomUUID().toString();
            Service service = new Service(serviceTemplateName, serviceName, serviceId);

            Journal journal = StarMgrJournal.logBootstrapService(service);
            journalSystem.write(journal);

            executeNoExceptionOrDie(() -> addService(service));

            LOG.info("service {} bootstrapped.", serviceName);
            return serviceId;
        }
    }

    /**
     * Shutdown a service
     *
     * @param serviceId
     *            service id
     *
     * @throws StarException
     *             if service does not exist
     */
    public void shutdownService(String serviceId) throws StarException {
        try (LockCloseable lock = new LockCloseable(writeLock())) {
            Service service = services.get(serviceId);
            if (service == null) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }

            // TODO: deal with journal write failure, it's ok for now
            if (service.setState(ServiceState.SHUTDOWN)) {
                Journal journal = StarMgrJournal.logShutdownService(service);
                journalSystem.write(journal);
            }

            LOG.info("service {} shutdown.", serviceId);
        }
    }

    /**
     * Get a service info
     *
     * @param serviceId
     *            service id
     *
     * @throws StarException
     *             if service does not exist
     */
    public ServiceInfo getServiceInfoById(String serviceId) throws StarException {
        try (LockCloseable lock = new LockCloseable(readLock())) {
            Service service = services.get(serviceId);
            if (service == null) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }

            return service.toProtobuf();
        }
    }

    /**
     * Get a service info
     *
     * @param serviceName
     *            service name
     *
     * @throws StarException
     *             if service does not exist
     */
    public ServiceInfo getServiceInfoByName(String serviceName) throws StarException {
        try (LockCloseable lock = new LockCloseable(readLock())) {
            for (Service service : services.values()) {
                if (service.getServiceName().equals(serviceName)) {
                    return service.toProtobuf();
                }
            }

            throw new StarException(ExceptionCode.NOT_EXIST,
                    String.format("service %s not exist.", serviceName));
        }
    }

    public int getServiceTemplateCount() {
        try (LockCloseable lock = new LockCloseable(readLock())) {
            return serviceTemplates.size();
        }
    }

    public int getServiceCount() {
        try (LockCloseable lock = new LockCloseable(readLock())) {
            return services.size();
        }
    }

    // lock must be held
    public boolean existService(String serviceId) {
        return services.get(serviceId) != null;
    }

    public Lock readLock() {
        return lock.readLock();
    }

    public Lock writeLock() {
        return lock.writeLock();
    }

    public void setShardScheduler(Scheduler shardScheduler) {
        this.shardScheduler = shardScheduler;
    }

    // add service to map and allocate a new shard manager if service not exist already
    private void addService(Service service) {
        Preconditions.checkState(lock.isWriteLocked());
        String serviceId = service.getServiceId();
        if (services.put(serviceId, service) == null) {
            ShardManager shardManager = new ShardManager(serviceId, journalSystem, idGenerator, shardScheduler);
            service.setShardManager(shardManager);
            FileStoreMgr fileStoreMgr = new FileStoreMgr(serviceId, journalSystem);
            service.setFileStoreMgr(fileStoreMgr);
        }
    }

    public ShardManager getShardManager(String serviceId) {
        try (LockCloseable lock = new LockCloseable(readLock())) {
            return getShardManagerNoLock(serviceId);
        }
    }

    // NOTE: The caller needs to take the responsibility to ensure the service available during
    //   the usage of the ShardManager
    public ShardManager getShardManagerNoLock(String serviceId) {
        Service service = services.get(serviceId);
        if (service == null) {
            return null;
        }
        return service.getShardManager();
    }

    public void replayRegisterService(ServiceTemplate serviceTemplate) {
        try (LockCloseable lock = new LockCloseable(writeLock())) {
            if (serviceTemplates.containsKey(serviceTemplate.getServiceTemplateName())) {
                LogUtils.fatal(LOG, "service template {} already exist when replay register service, should not happen!",
                        serviceTemplate.getServiceTemplateName());
            }

            serviceTemplates.put(serviceTemplate.getServiceTemplateName(), serviceTemplate);

            // TODO: state machine
        }
    }

    public void replayDeregisterService(String name) {
        try (LockCloseable lock = new LockCloseable(writeLock())) {
            if (!serviceTemplates.containsKey(name)) {
                LOG.warn("service template {} not exist when replay deregister service, just ignore.", name);
                return;
            }

            for (Service service : services.values()) {
                if (service.getServiceTemplateName().equals(name)) {
                    LogUtils.fatal(LOG,
                            "service template {} has running service when replay deregister service, should not happen!",
                            name);
                }
            }

            serviceTemplates.remove(name);

            // TODO: state machine
        }
    }

    public void replayBootstrapService(Service service) {
        try (LockCloseable lock = new LockCloseable(writeLock())) {
            if (!serviceTemplates.containsKey(service.getServiceTemplateName())) {
                LogUtils.fatal(LOG, "service template {} not exist when replay bootstrap service, should not happen!",
                        service.getServiceTemplateName());
            }

            if (services.containsKey(service.getServiceId())) {
                LogUtils.fatal(LOG, "service {} already exist when replay bootstrap service, should not happen!",
                        service.getServiceId());
            }

            addService(service);

            // TODO: state machine
        }
    }

    public void replayShutdownService(Service service) {
        try (LockCloseable lock = new LockCloseable(writeLock())) {
            if (!serviceTemplates.containsKey(service.getServiceTemplateName())) {
                LogUtils.fatal(LOG, "service template {} not exist when replay shutdown service, should not happen!",
                        service.getServiceTemplateName());
            }

            if (!services.containsKey(service.getServiceId())) {
                LogUtils.fatal(LOG, "service {} not exist when replay shutdown service, should not happen!",
                        service.getServiceId());
            }

            addService(service);

            // TODO: state machine
        }
    }

    public void replayUpdateFileStore(String serviceId, FileStoreInfo fsInfo) {
        try (LockCloseable ignored = new LockCloseable(readLock())) {
            Service service = services.get(serviceId);
            if (service == null) {
                LogUtils.fatal(LOG, "service {} not exist when replay update file store, should not happen!",
                        serviceId);
            }
            Preconditions.checkNotNull(service);
            FileStoreMgr fsMgr = service.getFileStoreMgr();
            Preconditions.checkNotNull(fsMgr);
            fsMgr.replayUpdateFileStore(fsInfo);
            updateShardManagerFileStoreSnapshotNoException(service, fsInfo.getFsKey());
        }
    }

    public void replayReplaceFileStore(String serviceId, FileStoreInfo fsInfo) {
        try (LockCloseable ignored = new LockCloseable(readLock())) {
            Service service = services.get(serviceId);
            if (service == null) {
                LogUtils.fatal(LOG, "service {} not exist when replay update file store, should not happen!",
                        serviceId);
            }
            Preconditions.checkNotNull(service);
            FileStoreMgr fsMgr = service.getFileStoreMgr();
            Preconditions.checkNotNull(fsMgr);
            fsMgr.replayReplaceFileStore(fsInfo);
            replaceShardManagerFileStoreSnapshotNoException(service, fsInfo.getFsKey());
        }
    }

    /**
     * Dump ServiceManager info into checkpoint image
     * @param out Output Stream
     * @throws IOException I/O exception
     * <pre>
     *  +----------------------+
     *  |  SERVICE_MGR_HEADER  | (number of service template, number of services)
     *  +----------------------+
     *  | SERVICE_MGR_SVC_TMPLS|
     *  +--------------------- +
     *  |       Templates      |
     *  +----------------------+
     *  |   SERVICE_MGR_SVCS   |
     *  +----------------------+
     *  |        Services      |
     *  +----------------------+
     *  | SERVICE_MGR_SHARD_MGR|
     *  +----------------------+
     *  |     SERVICE_ID_1     |
     *  |    SHARDMANAGER_1    |
     *  |     SERVICE_ID_2     |
     *  |    SHARDMANAGER_2    |
     *  |        ...           |
     *  |     SERVICE_ID_N     |
     *  |    SHARDMANAGER_N    |
     *  +----------------------+
     * </pre>
     */
    public void dumpMeta(OutputStream out) throws IOException {
        try (LockCloseable lk = new LockCloseable(readLock())) {
            LOG.debug("start dump service manager meta data ...");
            // write header
            ServiceManagerImageMetaHeader header = ServiceManagerImageMetaHeader.newBuilder()
                    .setDigestAlgorithm(Constant.DEFAULT_DIGEST_ALGORITHM)
                    .setNumServiceTemplate(serviceTemplates.size())
                    .setNumService(services.size())
                    .build();
            header.writeDelimitedTo(out);
            DigestOutputStream mdStream = Utils.getDigestOutputStream(out, Constant.DEFAULT_DIGEST_ALGORITHM);
            try (SectionWriter writer = new SectionWriter(mdStream)) {
                try (OutputStream stream = writer.appendSection(SectionType.SECTION_SERVICEMGR_TEMPLATE)) {
                    dumpServiceTemplates(stream);
                }
                try (OutputStream stream = writer.appendSection(SectionType.SECTION_SERVICEMGR_SERVICE)) {
                    dumpServices(stream);
                }
                for (String serviceId : services.keySet()) {
                    // each shardManager takes a separate section
                    try (OutputStream stream = writer.appendSection(SectionType.SECTION_SERVICEMGR_SHARDMGR)) {
                        dumpShardManager(stream, serviceId);
                    }
                    // each shardManager takes a separate section
                    try (OutputStream stream = writer.appendSection(SectionType.SECTION_SERVICEMGR_FILESTOREMGR)) {
                        dumpFileStoreMgr(stream, serviceId);
                    }
                }
            }
            // flush the mdStream because we are going to write to `out` directly.
            mdStream.flush();

            // write MetaFooter, DO NOT directly write new data here, change the protobuf definition
            ServiceManagerImageMetaFooter.Builder footerBuilder = ServiceManagerImageMetaFooter.newBuilder();
            if (mdStream.getMessageDigest() != null) {
                footerBuilder.setChecksum(ByteString.copyFrom(mdStream.getMessageDigest().digest()));
            }
            footerBuilder.build().writeDelimitedTo(out);
            LOG.debug("end dump service manager meta data.");
        }
    }

    public void loadMeta(InputStream in) throws IOException {
        try (LockCloseable lk = new LockCloseable(writeLock())) {
            LOG.debug("start load service manager meta data ...");
            ServiceManagerImageMetaHeader header = ServiceManagerImageMetaHeader.parseDelimitedFrom(in);
            if (header == null) {
                throw new EOFException();
            }
            DigestInputStream mdStream = Utils.getDigestInputStream(in, header.getDigestAlgorithm());
            try (SectionReader reader = new SectionReader(mdStream)) {
                reader.forEach(x -> loadMetaFromSection(x, header));
            }
            LOG.debug("end load service manager meta data.");
        }
    }

    private void loadMetaFromSection(Section section, ServiceManagerImageMetaHeader header) throws IOException {
        switch (section.getHeader().getSectionType()) {
            case SECTION_SERVICEMGR_TEMPLATE:
                loadServiceTemplates(section.getStream(), header.getNumServiceTemplate());
                break;
            case SECTION_SERVICEMGR_SERVICE:
                loadServices(section.getStream(), header.getNumService());
                break;
            case SECTION_SERVICEMGR_SHARDMGR:
                loadShardManager(section.getStream());
                break;
            case SECTION_SERVICEMGR_FILESTOREMGR:
                loadFileStoreMgr(section.getStream());
                break;
            default:
                LOG.warn("Unknown section type:{} when loadMeta in ServiceManager, ignore it!",
                        section.getHeader().getSectionType());
                break;
        }
    }

    private void dumpServiceTemplates(OutputStream stream) throws IOException {
        ServiceManagerImageData.Builder builder = ServiceManagerImageData.newBuilder();
        serviceTemplates.values().forEach(x -> builder.addData(x.toProtobuf().toByteString()));
        builder.build().writeDelimitedTo(stream);
    }

    private void loadServiceTemplates(InputStream stream, int numTemplates) throws IOException {
        ServiceManagerImageData templates = ServiceManagerImageData.parseDelimitedFrom(stream);
        if (templates == null) {
            throw new EOFException();
        }
        Preconditions.checkState(numTemplates == templates.getDataCount());
        for (ByteString bs : templates.getDataList()) {
            ServiceTemplate tmpl = ServiceTemplate.fromProtobuf(ServiceTemplateInfo.parseFrom(bs));
            serviceTemplates.put(tmpl.getServiceTemplateName(), tmpl);
        }
    }

    private void dumpServices(OutputStream stream) throws IOException {
        ServiceManagerImageData.Builder builder = ServiceManagerImageData.newBuilder();
        services.values().forEach(x -> builder.addData(x.toProtobuf().toByteString()));
        builder.build().writeDelimitedTo(stream);
    }

    private void loadServices(InputStream stream, int numServices) throws IOException {
        ServiceManagerImageData svcList = ServiceManagerImageData.parseDelimitedFrom(stream);
        if (svcList == null) {
            throw new EOFException();
        }
        Preconditions.checkState(numServices == svcList.getDataCount());
        for (ByteString bs : svcList.getDataList()) {
            Service svc = Service.fromProtobuf(ServiceInfo.parseFrom(bs));
            addService(svc);
        }
    }

    private void dumpShardManager(OutputStream stream, String serviceId) throws IOException {
        // write service id
        ServiceManagerShardManagerHeader.newBuilder().setServiceId(serviceId).build().writeDelimitedTo(stream);
        // write shardManager
        ShardManager manager = getShardManager(serviceId);
        manager.dumpMeta(stream);
    }

    private void loadShardManager(InputStream stream) throws IOException {
        ServiceManagerShardManagerHeader header = ServiceManagerShardManagerHeader.parseDelimitedFrom(stream);
        if (header == null) {
            throw new EOFException();
        }
        ShardManager manager = getShardManager(header.getServiceId());
        manager.loadMeta(stream);
    }

    public Set<String> getServiceIdSet() {
        try (LockCloseable lk = new LockCloseable(readLock())) {
            return new HashSet<>(services.keySet());
        }
    }

    public void updateFileStore(String serviceId, FileStoreInfo fsInfo) {
        FileStoreMgr fsMgr = getFileStoreMgr(serviceId);
        Preconditions.checkNotNull(fsMgr);
        FileStore fs = FileStore.fromProtobuf(fsInfo);
        Preconditions.checkNotNull(fs);
        if (fs.type() == FileStoreType.INVALID) {
            throw new InvalidArgumentStarException(String.format("invalid file store type %s", fsInfo.getFsType()));
        }
        // Triggers EDIT LOG WRITE
        fsMgr.updateFileStore(fs);

        updateShardManagerFileStoreSnapshotNoException(services.get(serviceId), fs.key());
    }

    public void replaceFileStore(String serviceId, FileStoreInfo fsInfo) {
        FileStoreMgr fsMgr = getFileStoreMgr(serviceId);
        Preconditions.checkNotNull(fsMgr);
        FileStore fs = FileStore.fromProtobuf(fsInfo);
        Preconditions.checkNotNull(fs);
        if (fs.type() == FileStoreType.INVALID) {
            throw new InvalidArgumentStarException("invalid file store type {}", fsInfo.getFsType());
        }
        // Triggers EDIT LOG WRITE
        fsMgr.replaceFileStore(fs);

        replaceShardManagerFileStoreSnapshotNoException(services.get(serviceId), fs.key());
    }

    private void updateShardManagerFileStoreSnapshotNoException(Service service, String fsKey) {
        ShardManager shardManager = service.getShardManager();
        if (shardManager != null) {
            try {
                FileStoreMgr fsMgr = service.getFileStoreMgr();
                shardManager.updateDelegatedFileStoreSnapshot(fsMgr.getFileStore(fsKey));
            } catch (Throwable throwable) {
                LOG.warn("Fail to update shardManager delegatedFileStoreSnapshot fsKey={}. Error: ", fsKey, throwable);
            }
        }
    }

    private void replaceShardManagerFileStoreSnapshotNoException(Service service, String fsKey) {
        ShardManager shardManager = service.getShardManager();
        if (shardManager != null) {
            try {
                FileStoreMgr fsMgr = service.getFileStoreMgr();
                shardManager.replaceDelegatedFileStoreSnapshot(fsMgr.getFileStore(fsKey));
            } catch (Throwable throwable) {
                LOG.warn("Fail to replace shardManager delegatedFileStoreSnapshot fsKey={}. Error: ", fsKey, throwable);
            }
        }
    }

    public void dump(DataOutputStream out) throws IOException {
        try (LockCloseable lk = new LockCloseable(readLock())) {
            for (ServiceTemplate template : serviceTemplates.values()) {
                String s = JsonFormat.printer().print(template.toProtobuf()) + "\n";
                out.writeBytes(s);
            }
            for (Service service : services.values()) {
                String s = JsonFormat.printer().print(service.toProtobuf()) + "\n";
                out.writeBytes(s);

                service.getShardManager().dump(out);
                service.getFileStoreMgr().dump(out);
            }
        }
    }

    public FileStoreMgr getFileStoreMgr(String serviceId) throws StarException {
        try (LockCloseable lock = new LockCloseable(readLock())) {
            if (!services.containsKey(serviceId)) {
                throw new NotExistStarException(String.format("service %s not exist.", serviceId));
            }
            Service service = services.get(serviceId);
            return service.getFileStoreMgr();
        }
    }

    private void dumpFileStoreMgr(OutputStream stream, String serviceId) throws IOException {
        // write service id
        ServiceManagerFileStoreMgrHeader.newBuilder().setServiceId(serviceId).build().writeDelimitedTo(stream);
        // write fileStoreMgr
        FileStoreMgr fsMgr = getFileStoreMgr(serviceId);
        fsMgr.dumpMeta(stream);
    }

    private void loadFileStoreMgr(InputStream stream) throws IOException {
        ServiceManagerFileStoreMgrHeader header = ServiceManagerFileStoreMgrHeader.parseDelimitedFrom(stream);
        if (header == null) {
            throw new EOFException();
        }
        FileStoreMgr fsMgr = getFileStoreMgr(header.getServiceId());
        fsMgr.loadMeta(stream);
    }
}
