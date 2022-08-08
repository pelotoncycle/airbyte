package io.airbyte.integrations.destination.redshift.consumers;

import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.destination.buffered_stream_consumer.OnCloseFunction;
import io.airbyte.integrations.destination.jdbc.WriteConfig;
import io.airbyte.integrations.destination.redshift.operations.RedshiftS3StagingSqlOperations;
import io.airbyte.integrations.destination.staging.StagingConsumerFactory;
import io.airbyte.integrations.destination.staging.StagingOperations;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedshiftStagingConsumerFactory extends StagingConsumerFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftStagingConsumerFactory.class);

  @Override
  public OnCloseFunction onCloseFunction(final JdbcDatabase database,
      final StagingOperations stagingOperations,
      final List<WriteConfig> writeConfigs,
      final boolean purgeStagingData) {
    return (hasFailed) -> {
      if (!hasFailed) {
        final List<String> queryList = new ArrayList<>();
        LOGGER.info("Copying into tables in destination started for {} streams", writeConfigs.size());
        for (final WriteConfig writeConfig : writeConfigs) {
          final String schemaName = writeConfig.getOutputSchemaName();
          final String streamName = writeConfig.getStreamName();
          final String srcTableName = writeConfig.getTmpTableName();
          final String dstTableName = writeConfig.getOutputTableName();

          final String stageName = stagingOperations.getStageName(schemaName, streamName);
          final String stagingPath = stagingOperations.getStagingPath(RANDOM_CONNECTION_ID, schemaName, streamName, writeConfig.getWriteDatetime());
          LOGGER.info("Copying stream {} of schema {} into tmp table {} to final table {} from stage path {} with {} file(s) [{}]",
              streamName, schemaName, srcTableName, dstTableName, stagingPath, writeConfig.getStagedFiles().size(),
              String.join(",", writeConfig.getStagedFiles()));

          try {
            stagingOperations.copyIntoTmpTableFromStage(database, stageName, stagingPath, writeConfig.getStagedFiles(), srcTableName, schemaName);
          } catch (final Exception e) {
            stagingOperations.cleanUpStage(database, stageName, writeConfig.getStagedFiles());
            LOGGER.info("Cleaning stage path {}", stagingPath);
            throw new RuntimeException("Failed to upload data from stage " + stagingPath, e);
          }
          writeConfig.clearStagedFiles();
          stagingOperations.createTableIfNotExists(database, schemaName, dstTableName);
          final String mode = String.format("Got sync mode %s",writeConfig.getSyncMode().value());
          LOGGER.info(mode);
          switch (writeConfig.getSyncMode()) {
            case OVERWRITE -> {
              queryList.add(stagingOperations.truncateTableQuery(database, schemaName, dstTableName));
              queryList.add(stagingOperations.copyTableQuery(database, schemaName, srcTableName, dstTableName));
            }
            case APPEND -> {
              queryList.add(stagingOperations.copyTableQuery(database, schemaName, srcTableName, dstTableName));
            }
            case APPEND_DEDUP -> {
              final String deleteQuery = stagingOperations.deleteFromTableQuery(database,schemaName,srcTableName,dstTableName,writeConfig.getPrimaryKeys());
              LOGGER.info(String.format("adding delete query: %s", deleteQuery));
              queryList.add("SET enable_case_sensitive_identifier TO true;");
              queryList.add(deleteQuery);
              final RedshiftS3StagingSqlOperations operations = (RedshiftS3StagingSqlOperations) stagingOperations;
              final String copyTableQuery = operations.copyTableQuery(database, schemaName, srcTableName, dstTableName,writeConfig.getPrimaryKeys());
              queryList.add(copyTableQuery);
              queryList.add("SET enable_case_sensitive_identifier TO false;");

            }
            default -> throw new IllegalStateException("Unrecognized sync mode: " + writeConfig.getSyncMode());
          }

        }
        stagingOperations.onDestinationCloseOperations(database, writeConfigs);
        LOGGER.info("Executing finalization of tables.");
        stagingOperations.executeTransaction(database, queryList);
        LOGGER.info("Finalizing tables in destination completed.");
      }
      LOGGER.info("Cleaning up destination started for {} streams", writeConfigs.size());
      for (final WriteConfig writeConfig : writeConfigs) {
        final String schemaName = writeConfig.getOutputSchemaName();
        final String tmpTableName = writeConfig.getTmpTableName();
        LOGGER.info("Cleaning tmp table in destination started for stream {}. schema {}, tmp table name: {}", writeConfig.getStreamName(), schemaName,
            tmpTableName);

        stagingOperations.dropTableIfExists(database, schemaName, tmpTableName);
        if (purgeStagingData) {
          final String stageName = stagingOperations.getStageName(schemaName, writeConfig.getStreamName());
          LOGGER.info("Cleaning stage in destination started for stream {}. schema {}, stage: {}", writeConfig.getStreamName(), schemaName,
              stageName);
          stagingOperations.dropStageIfExists(database, stageName);
        }
      }
      LOGGER.info("Cleaning up destination completed.");
    };
  }
}
