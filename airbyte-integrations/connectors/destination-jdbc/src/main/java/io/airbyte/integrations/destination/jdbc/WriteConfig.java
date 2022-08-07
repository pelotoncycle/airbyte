/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.jdbc;

import io.airbyte.protocol.models.DestinationSyncMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Write configuration POJO for all destinations extending {@link AbstractJdbcDestination}.
 */
public class WriteConfig {

  private final String streamName;

  private final String namespace;

  private final String outputSchemaName;
  private final String tmpTableName;
  private final String outputTableName;
  private final DestinationSyncMode syncMode;
  private final DateTime writeDatetime;
  private final List<String> stagedFiles;

  private final List<List<String>> primaryKeys;

  public WriteConfig(final String streamName,
                     final String namespace,
                     final String outputSchemaName,
                     final String tmpTableName,
                     final String outputTableName,
                     final List<List<String>> primaryKeys,
                     final DestinationSyncMode syncMode) {
    this(streamName, namespace, outputSchemaName, tmpTableName, outputTableName, primaryKeys, syncMode, DateTime.now(DateTimeZone.UTC));
  }

  public WriteConfig(final String streamName,
                     final String namespace,
                     final String outputSchemaName,
                     final String tmpTableName,
                     final String outputTableName,
                     final List<List<String>> primaryKeys,
                     final DestinationSyncMode syncMode,
                     final DateTime writeDatetime) {
    this.streamName = streamName;
    this.namespace = namespace;
    this.outputSchemaName = outputSchemaName;
    this.tmpTableName = tmpTableName;
    this.outputTableName = outputTableName;
    this.syncMode = syncMode;
    this.stagedFiles = new ArrayList<>();
    this.primaryKeys = primaryKeys;
    this.writeDatetime = writeDatetime;
  }

  public String getStreamName() {
    return streamName;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getTmpTableName() {
    return tmpTableName;
  }

  public String getOutputSchemaName() {
    return outputSchemaName;
  }

  public String getOutputTableName() {
    return outputTableName;
  }

  public DestinationSyncMode getSyncMode() {
    return syncMode;
  }

  public DateTime getWriteDatetime() {
    return writeDatetime;
  }

  public List<String> getStagedFiles() {
    return stagedFiles;
  }

  public void addStagedFile(final String file) {
    stagedFiles.add(file);
  }

  public void clearStagedFiles() {
    stagedFiles.clear();
  }
  public List<List<String>> getPrimaryKeys() {
    return primaryKeys;
  }
  @Override
  public String toString() {

    return "WriteConfig{" +
        "streamName=" + streamName +
        ", namespace=" + namespace +
        ", outputSchemaName=" + outputSchemaName +
        ", tmpTableName=" + tmpTableName +
        ", outputTableName=" + outputTableName +
        ", syncMode=" + syncMode +
        ", primaryKeys=" + getPrimaryKeys().stream().map( keys -> String.join(".", keys)).collect(Collectors.joining(", ")) +
        '}';
  }

}
