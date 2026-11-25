// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.catalog.iceberg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.util.JsonUtil;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.util.IcebergSchemaConverter;
import org.junit.rules.TemporaryFolder;

/**
 * Iceberg table in a HadoopCatalog layout (<warehouse>/<db>/<name>) inside a JUnit
 * temporary folder, with builders for the snapshots tests need. Data files are
 * placeholders of the declared size, not real Parquet or Avro: table loading only reads
 * their file status.
 */
public class IcebergTestTables {
  private static final String WAREHOUSE_DIR = "iceberg-warehouse";

  private final String db_;
  private final String name_;
  private final String warehouse_;
  private final Table table_;
  /** Values declared for data files, by file location and then by column name. */
  private final Map<String, Map<String, List<Object>>> columnValues_ = new HashMap<>();

  private IcebergTestTables(String db, String name, String warehouse, Table table) {
    db_ = db;
    name_ = name;
    warehouse_ = warehouse;
    table_ = table;
  }

  public static IcebergTestTables create(TemporaryFolder tmp, String db, String name,
      Schema schema, PartitionSpec spec, int formatVersion) {
    File warehouseDir = new File(tmp.getRoot(), WAREHOUSE_DIR);
    File tableDir = new File(new File(warehouseDir, db), name);
    Map<String, String> props = new HashMap<>();
    props.put(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion));
    Table table = new HadoopTables(FileSystemUtil.getConfiguration())
        .create(schema, spec, props, qualified(tableDir));
    return new IcebergTestTables(db, name, qualified(warehouseDir), table);
  }

  /**
   * Wraps 'table' of another catalog, e.g. the HiveCatalog, so that the builders of this
   * class work on it; only msTable() needs a HadoopCatalog table.
   */
  public static IcebergTestTables of(String db, String name, Table table) {
    return new IcebergTestTables(db, name, null, table);
  }

  /**
   * Hadoop's normalized form ("file:/..."): Impala compares data file paths with the
   * table location as strings, so every location here must use the same form.
   */
  private static String qualified(File dir) {
    return new Path(dir.getAbsoluteFile().toURI()).toString();
  }

  public DataFile file(long sizeBytes, String partitionPath) {
    return file(sizeBytes, partitionPath, FileFormat.PARQUET);
  }

  public DataFile file(long sizeBytes, String partitionPath, FileFormat format) {
    return placeholderFile(table_, sizeBytes, partitionPath, format);
  }

  /**
   * Placeholder data file whose rows hold 'columnValues': column name to one value per
   * row, as Java objects of Iceberg's generic data model. The record count is the number
   * of rows; columns left out are NULL in every row.
   */
  public DataFile file(long sizeBytes, String partitionPath,
      Map<String, ? extends List<?>> columnValues) {
    Preconditions.checkArgument(!columnValues.isEmpty(), "no column values");
    int rows = -1;
    Map<String, List<Object>> declared = new HashMap<>();
    for (Map.Entry<String, ? extends List<?>> e : columnValues.entrySet()) {
      Preconditions.checkArgument(table_.schema().findField(e.getKey()) != null,
          "no column %s", e.getKey());
      Preconditions.checkArgument(rows < 0 || rows == e.getValue().size(),
          "column %s has %s values, other columns %s", e.getKey(), e.getValue().size(),
          rows);
      rows = e.getValue().size();
      declared.put(e.getKey(),
          Collections.unmodifiableList(new ArrayList<>(e.getValue())));
    }
    DataFile file = placeholderFile(table_, sizeBytes, partitionPath, FileFormat.PARQUET,
        rows);
    columnValues_.put(file.location(), declared);
    return file;
  }

  /** Values of 'column' in the rows of a data file made with declared values. */
  public List<?> columnValues(String path, String column) {
    Map<String, List<Object>> declared = columnValues_.get(path);
    Preconditions.checkArgument(declared != null, "no values declared for %s", path);
    Preconditions.checkArgument(table_.schema().findField(column) != null,
        "no column %s", column);
    List<Object> values = declared.get(column);
    if (values != null) return values;
    int rows = declared.values().iterator().next().size();
    return Collections.nCopies(rows, null);
  }

  public DeleteFile positionDeletes(long sizeBytes, long recordCount,
      String partitionPath) {
    return deleteFile(FileMetadata.deleteFileBuilder(table_.spec()).ofPositionDeletes(),
        sizeBytes, recordCount, partitionPath);
  }

  public DeleteFile equalityDeletes(long sizeBytes, long recordCount,
      String partitionPath, int... fieldIds) {
    return deleteFile(
        FileMetadata.deleteFileBuilder(table_.spec()).ofEqualityDeletes(fieldIds),
        sizeBytes, recordCount, partitionPath);
  }

  private DeleteFile deleteFile(FileMetadata.Builder builder, long sizeBytes,
      long recordCount, String partitionPath) {
    builder.withPath(placeholder(table_, sizeBytes, partitionPath, FileFormat.PARQUET))
        .withFileSizeInBytes(sizeBytes)
        .withRecordCount(recordCount)
        .withFormat(FileFormat.PARQUET);
    if (partitionPath != null) builder.withPartitionPath(partitionPath);
    return builder.build();
  }

  public long append(DataFile... files) {
    return append(table_, files);
  }

  /**
   * Placeholder data file of 'table', which may belong to any catalog, under
   * <location>/data. Every call makes a file with a new name.
   */
  public static DataFile placeholderFile(Table table, long sizeBytes,
      String partitionPath, FileFormat format) {
    return placeholderFile(table, sizeBytes, partitionPath, format, 1);
  }

  private static DataFile placeholderFile(Table table, long sizeBytes,
      String partitionPath, FileFormat format, long recordCount) {
    DataFiles.Builder builder = DataFiles.builder(table.spec())
        .withPath(placeholder(table, sizeBytes, partitionPath, format))
        .withFileSizeInBytes(sizeBytes)
        .withRecordCount(recordCount)
        .withFormat(format);
    if (partitionPath != null) builder.withPartitionPath(partitionPath);
    return builder.build();
  }

  private static String placeholder(Table table, long sizeBytes, String partitionPath,
      FileFormat format) {
    String dir = table.location() + "/data"
        + (partitionPath == null ? "" : "/" + partitionPath);
    String path = dir + "/" + UUID.randomUUID() + "." + format.name().toLowerCase();
    File local = new File(new Path(path).toUri().getPath());
    assertTrue("cannot create " + local.getParent(),
        local.getParentFile().isDirectory() || local.getParentFile().mkdirs());
    try (RandomAccessFile f = new RandomAccessFile(local, "rw")) {
      f.setLength(sizeBytes);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    assertEquals("size of " + local, sizeBytes, local.length());
    return path;
  }

  /** Commits 'files' to 'table' of any catalog as a new snapshot and returns its id. */
  public static long append(Table table, DataFile... files) {
    AppendFiles append = table.newAppend();
    for (DataFile f : files) append.appendFile(f);
    return commit(table, append, null);
  }

  private static long commit(Table table, SnapshotUpdate<?> update, String branch) {
    table.refresh();
    long latest = Long.MIN_VALUE;
    for (Snapshot s : table.snapshots()) latest = Math.max(latest, s.timestampMillis());
    // Tests order HMS and Puffin stats by snapshot time, so ties must not happen.
    while (System.currentTimeMillis() <= latest) Thread.onSpinWait();
    if (branch != null) update.toBranch(branch);
    update.commit();
    table.refresh();
    Snapshot current = branch == null ? table.currentSnapshot() : table.snapshot(branch);
    assertTrue("snapshot time did not advance", current.timestampMillis() > latest);
    return current.snapshotId();
  }

  /**
   * Appends 'files' to 'branch' and returns the new snapshot id. A missing branch is
   * created at the current snapshot of main first.
   */
  public long appendToBranch(String branch, DataFile... files) {
    table_.refresh();
    if (!table_.refs().containsKey(branch)) {
      Snapshot current = table_.currentSnapshot();
      Preconditions.checkState(current != null, "no snapshot to branch from");
      table_.manageSnapshots().createBranch(branch, current.snapshotId()).commit();
    }
    AppendFiles append = table_.newAppend();
    for (DataFile f : files) append.appendFile(f);
    return commit(table_, append, branch);
  }

  public void rollbackTo(long snapshotId) {
    table_.manageSnapshots().rollbackTo(snapshotId).commit();
  }

  /**
   * Commits a compaction-like 'replace' snapshot rewriting 'removed' into 'added'. As
   * compaction, it checks only deletes committed after the current snapshot: without a
   * starting snapshot Iceberg rejects a file that any earlier delete file may apply to.
   */
  public long replace(List<DataFile> removed, List<DataFile> added) {
    RewriteFiles rewrite = table_.newRewrite();
    table_.refresh();
    Snapshot current = table_.currentSnapshot();
    if (current != null) rewrite.validateFromSnapshot(current.snapshotId());
    for (DataFile f : removed) rewrite.deleteFile(f);
    for (DataFile f : added) rewrite.addFile(f);
    return commit(table_, rewrite, null);
  }

  public long overwrite(List<DataFile> removed, List<DataFile> added) {
    OverwriteFiles overwrite = table_.newOverwrite();
    for (DataFile f : removed) overwrite.deleteFile(f);
    for (DataFile f : added) overwrite.addFile(f);
    return commit(table_, overwrite, null);
  }

  public long delete(DataFile... files) {
    DeleteFiles delete = table_.newDelete();
    for (DataFile f : files) delete.deleteFile(f);
    return commit(table_, delete, null);
  }

  /**
   * Commits 'deletes' in one row delta. Its summary counts what the delete files declare;
   * setSummary() replaces the counters.
   */
  public long rowDelta(DeleteFile... deletes) {
    RowDelta delta = table_.newRowDelta();
    for (DeleteFile f : deletes) delta.addDeletes(f);
    return commit(table_, delta, null);
  }

  public void expire(long snapshotId) {
    Snapshot current = table().currentSnapshot();
    Preconditions.checkArgument(current == null || current.snapshotId() != snapshotId,
        "current snapshot %s cannot be expired", snapshotId);
    table_.expireSnapshots().expireSnapshotId(snapshotId).commit();
  }

  public void setStats(long snapshotId, StatisticsFile file) {
    Preconditions.checkArgument(file.snapshotId() == snapshotId,
        "statistics file of snapshot %s registered for %s", file.snapshotId(), snapshotId);
    table_.updateStatistics().setStatistics(file).commit();
  }

  /**
   * Commits metadata whose snapshots have no schema id, as snapshots written by old
   * Iceberg versions.
   */
  public void removeSnapshotSchemaIds() {
    editMetadataJson(json -> {
      for (JsonNode snapshot : json.get("snapshots")) {
        ((ObjectNode) snapshot).remove("schema-id");
      }
    });
  }

  /**
   * Sets the schema id of a committed snapshot, which may name no schema of the table;
   * null removes it.
   */
  public void setSnapshotSchemaId(long snapshotId, Integer schemaId) {
    editSnapshotJson(snapshotId, snapshot -> {
      if (schemaId == null) {
        snapshot.remove("schema-id");
      } else {
        snapshot.put("schema-id", schemaId);
      }
    });
  }

  /**
   * Puts 'entries' into the summary of a committed snapshot. Values are kept as given,
   * so a counter may be any string.
   */
  public void setSummary(long snapshotId, Map<String, String> entries) {
    editSnapshotJson(snapshotId, snapshot -> {
      Preconditions.checkState(snapshot.get("summary") != null,
          "snapshot %s has no summary", snapshotId);
      ObjectNode summary = (ObjectNode) snapshot.get("summary");
      for (Map.Entry<String, String> e : entries.entrySet()) {
        summary.put(e.getKey(), e.getValue());
      }
    });
  }

  /**
   * Removes 'keys' from the summary of a committed snapshot. Iceberg reads a summary
   * without "operation" as an overwrite, so removeSummary() drops the operation.
   */
  public void removeFromSummary(long snapshotId, String... keys) {
    Preconditions.checkArgument(!Arrays.asList(keys).contains("operation"),
        "Iceberg would read the operation as overwrite, use removeSummary()");
    editSnapshotJson(snapshotId, snapshot -> {
      Preconditions.checkState(snapshot.get("summary") != null,
          "snapshot %s has no summary", snapshotId);
      ObjectNode summary = (ObjectNode) snapshot.get("summary");
      for (String key : keys) summary.remove(key);
    });
  }

  /** Drops the summary of a committed snapshot: operation() and summary() are null. */
  public void removeSummary(long snapshotId) {
    editSnapshotJson(snapshotId, snapshot -> {
      Preconditions.checkState(snapshot.remove("summary") != null,
          "snapshot %s has no summary", snapshotId);
    });
  }

  /**
   * Turns a committed snapshot into one without a manifest list, as early v1 writers
   * made them: the snapshot lists its manifests itself. Iceberg then attributes every
   * listed manifest to this snapshot, so only a snapshot whose manifests are all its own
   * keeps its added and removed files.
   */
  public void removeManifestList(long snapshotId) {
    Snapshot snapshot = table().snapshot(snapshotId);
    Preconditions.checkArgument(snapshot != null, "no snapshot %s", snapshotId);
    List<String> manifests = new ArrayList<>();
    for (ManifestFile m : snapshot.allManifests(table_.io())) manifests.add(m.path());
    editSnapshotJson(snapshotId, json -> {
      Preconditions.checkState(json.remove("manifest-list") != null,
          "snapshot %s has no manifest list", snapshotId);
      ArrayNode list = json.putArray("manifests");
      for (String m : manifests) list.add(m);
    });
  }

  private void editSnapshotJson(long snapshotId, Consumer<ObjectNode> edit) {
    editMetadataJson(json -> {
      for (JsonNode snapshot : json.get("snapshots")) {
        if (snapshot.get("snapshot-id").asLong() == snapshotId) {
          edit.accept((ObjectNode) snapshot);
          return;
        }
      }
      throw new IllegalArgumentException("no snapshot " + snapshotId);
    });
  }

  /**
   * Commits metadata.json edited by 'edit': the Iceberg API cannot make the metadata of
   * old or foreign writers.
   */
  private void editMetadataJson(Consumer<ObjectNode> edit) {
    TableOperations ops = ((BaseTable) table_).operations();
    TableMetadata base = ops.refresh();
    try {
      ObjectNode json =
          (ObjectNode) JsonUtil.mapper().readTree(TableMetadataParser.toJson(base));
      edit.accept(json);
      ops.commit(base, TableMetadataParser.fromJson(base.metadataFileLocation(), json));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public Table table() {
    table_.refresh();
    return table_;
  }

  public String db() { return db_; }

  public String name() { return name_; }

  /**
   * HMS table of an external hadoop.catalog Iceberg table pointing at this table.
   * 'params' are added on top of the Iceberg parameters.
   */
  public org.apache.hadoop.hive.metastore.api.Table msTable(Map<String, String> params) {
    Preconditions.checkState(warehouse_ != null, "%s.%s is not a HadoopCatalog table",
        db_, name_);
    Map<String, String> tblParams = new HashMap<>();
    tblParams.put("EXTERNAL", "TRUE");
    tblParams.put(IcebergTable.KEY_STORAGE_HANDLER, IcebergTable.ICEBERG_STORAGE_HANDLER);
    tblParams.put(IcebergTable.ICEBERG_CATALOG, "hadoop.catalog");
    tblParams.put(IcebergTable.ICEBERG_CATALOG_LOCATION, warehouse_);
    tblParams.put(IcebergTable.ICEBERG_TABLE_IDENTIFIER, db_ + "." + name_);
    tblParams.putAll(params);

    SerDeInfo serde = new SerDeInfo();
    serde.setSerializationLib(HdfsFileFormat.ICEBERG.serializationLib());
    serde.setParameters(new HashMap<>());
    StorageDescriptor sd = new StorageDescriptor();
    try {
      sd.setCols(IcebergSchemaConverter.convertToHiveSchema(table().schema()));
    } catch (ImpalaRuntimeException e) {
      throw new IllegalStateException("schema has no Hive equivalent", e);
    }
    sd.setLocation(table_.location());
    sd.setInputFormat(HdfsFileFormat.ICEBERG.inputFormat());
    sd.setOutputFormat(HdfsFileFormat.ICEBERG.outputFormat());
    sd.setSerdeInfo(serde);
    sd.setCompressed(false);
    sd.setNumBuckets(-1);
    sd.setBucketCols(new ArrayList<>());
    sd.setSortCols(new ArrayList<>());
    sd.setParameters(new HashMap<>());

    org.apache.hadoop.hive.metastore.api.Table msTbl =
        new org.apache.hadoop.hive.metastore.api.Table();
    msTbl.setDbName(db_);
    msTbl.setTableName(name_);
    msTbl.setOwner(System.getProperty("user.name"));
    msTbl.setTableType(TableType.EXTERNAL_TABLE.toString());
    msTbl.setPartitionKeys(new ArrayList<>());
    msTbl.setParameters(tblParams);
    msTbl.setSd(sd);
    return msTbl;
  }
}
