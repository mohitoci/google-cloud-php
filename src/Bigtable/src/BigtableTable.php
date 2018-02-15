<?php
/*
 * Copyright 2017, Google LLC All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Google\Cloud\Bigtable\src;

use Google\Cloud\Bigtable\Admin\V2\ColumnFamily;
use Google\Cloud\Bigtable\Admin\V2\GcRule;
use Google\Cloud\Bigtable\Admin\V2\ModifyColumnFamiliesRequest_Modification as Modification;
use Google\Cloud\Bigtable\Admin\V2\Table;
use Google\Cloud\Bigtable\Admin\V2\BigtableTableAdminClient;

use Google\Cloud\Bigtable\V2\Mutation;
use Google\Cloud\Bigtable\V2\Mutation_SetCell;
use Google\Cloud\Bigtable\V2\MutateRowsRequest_Entry;
use Google\Cloud\Bigtable\V2\ReadModifyWriteRule;

use Google\Cloud\Bigtable\V2\BigtableClient;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\MapField;

use Google\GAX\ValidationException;

/**
 *
 */
class BigtableTable
{
    private $BigtableClient;
    private $BigtableTableAdminClient;
    private $projectId;
    private $instanceId;
    private $tableId;
    private $formattedInstance;

    /**
     * Constructor.
     * @param array $args {
     *
     *     @param string $projectId
     *
     *     @param string $instanceId
     */
    public function __construct($args)
    {
        $this->projectId = $args['projectId'];
        $this->instanceId = $args['instanceId'];
        $this->formattedInstance = BigtableTableAdminClient::instanceName($this->projectId, $this->instanceId);

        $this->BigtableClient = new BigtableClient();
        $this->BigtableTableAdminClient = new BigtableTableAdminClient();
    }

    /**
     * Formats a string containing the fully-qualified path to represent
     *
     * @param string $projectId
     * @param string $instanceId
     * @param string $tableId
     *
     * @return string The formatted table resource.
     */
    private function tableName($tableId)
    {
        return BigtableTableAdminClient::tableName($this->projectId, $this->instanceId, $tableId);
    }

    /**
     * Creates a new table in the specified instance.
     *
     * @param string $tableId      The name by which the new table should be referred to within the parent
     *                             instance, e.g., `foobar` rather than `<parent>/tables/foobar`.
     *
     * @param array  $optionalArgs {
     *                             Optional.
     *
     *     @type Split[] $initialSplits
     *          The optional list of row keys that will be used to initially split the
     *          table into several tablets (tablets are similar to HBase regions).
     *          Given two split keys, `s1` and `s2`, three tablets will be created,
     *          spanning the key ranges: `[, s1), [s1, s2), [s2, )`.
     *
     *          Example:
     *
     *          * Row keys := `["a", "apple", "custom", "customer_1", "customer_2",`
     *                         `"other", "zz"]`
     *          * initial_split_keys := `["apple", "customer_1", "customer_2", "other"]`
     *          * Key assignment:
     *              - Tablet 1 `[, apple)                => {"a"}.`
     *              - Tablet 2 `[apple, customer_1)      => {"apple", "custom"}.`
     *              - Tablet 3 `[customer_1, customer_2) => {"customer_1"}.`
     *              - Tablet 4 `[customer_2, other)      => {"customer_2"}.`
     *              - Tablet 5 `[other, )                => {"other", "zz"}.`
     *     @type \Google\GAX\RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\GAX\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\GAX\RetrySettings} for example usage.
     * }
     * @return \Google\Bigtable\Admin\V2\Table
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     */
    public function createTable($tableId, $optionalArgs = [])
    {
        $parent = $this->formattedInstance;
        $Table = $this->BigtableTableAdminClient->createTable($parent, $tableId, new Table(), $optionalArgs);
        return $Table;
    }

    /**
     * Creates a new table in the specified instance with column family.
     *
     * @param string $tableId      The name by which the new table should be referred to within the parent
     *                             instance, e.g., `foobar` rather than `<parent>/tables/foobar`.
     * @param string $columnFamily e.g., `cf`
     *
     * @param array  $optionalArgs {
     *                             Optional.
     *
     *     @type Split[] $initialSplits
     *          The optional list of row keys that will be used to initially split the
     *          table into several tablets (tablets are similar to HBase regions).
     *          Given two split keys, `s1` and `s2`, three tablets will be created,
     *          spanning the key ranges: `[, s1), [s1, s2), [s2, )`.
     *
     *          Example:
     *
     *          * Row keys := `["a", "apple", "custom", "customer_1", "customer_2",`
     *                         `"other", "zz"]`
     *          * initial_split_keys := `["apple", "customer_1", "customer_2", "other"]`
     *          * Key assignment:
     *              - Tablet 1 `[, apple)                => {"a"}.`
     *              - Tablet 2 `[apple, customer_1)      => {"apple", "custom"}.`
     *              - Tablet 3 `[customer_1, customer_2) => {"customer_1"}.`
     *              - Tablet 4 `[customer_2, other)      => {"customer_2"}.`
     *              - Tablet 5 `[other, )                => {"other", "zz"}.`
     *     @type \Google\GAX\RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\GAX\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\GAX\RetrySettings} for example usage.
     * }
     * @return \Google\Bigtable\Admin\V2\Table
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     */
    public function createTableWithColumnFamily($tableId, $columnFamily, $optionalArgs = [])
    {
        $table = new Table();
        $table->setGranularity(3);

        $MapField = $this->columnFamily(3, $columnFamily);
        $table->setColumnFamilies($MapField);

        $parent = $this->formattedInstance;
        $Table = $this->BigtableTableAdminClient->createTable($parent, $tableId, $table, $optionalArgs);
        return $Table;
    }

    /**
     * Creates a new table in the specified instance with column family.
     * @param integer $MaxNumVersions
     *
     * @param string $columnFamily e.g., `cf`
     *
     * @return \Google\Protobuf\Internal\MapField
     */
    public function columnFamily($MaxNumVersions, $columnFamily)
    {
        $gc = new GcRule();
        $gc->setMaxNumVersions($MaxNumVersions);

        $cf = new ColumnFamily();
        $cf->setGcRule($gc);

        $MapField = new MapField(GPBType::STRING, GPBType::MESSAGE, ColumnFamily::class);
        $MapField[$columnFamily] = $cf;
        return $MapField;
    }

    /**
     * Permanently deletes a specified table and all of its data.
     *
     * @param string $tableId       The table should be deleted from the parent instance
     *
     * @param array  $optionalArgs {
     *                             Optional.
     *
     *     @type \Google\GAX\RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\GAX\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\GAX\RetrySettings} for example usage.
     * }
     *
     *
     * @return \Google\Protobuf\GPBEmpty
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     */
    public function deleteTable($tableId, $optionalArgs = [])
    {
        $formattedTable = $this->tableName($tableId);
        return $this->BigtableTableAdminClient->deleteTable($formattedTable, $optionalArgs);
    }

    /**
     * Lists all tables served from a specified instance.
     *
     * @param array  $optionalArgs {
     *                             Optional.
     *
     *     @type int $view
     *          The view to be applied to the returned tables' fields.
     *          Defaults to `NAME_ONLY` if unspecified; no others are currently supported.
     *          For allowed values, use constants defined on {@see \Google\Bigtable\Admin\V2\Table_View}
     *     @type string $pageToken
     *          A page token is used to specify a page of values to be returned.
     *          If no page token is specified (the default), the first page
     *          of values will be returned. Any page token used here must have
     *          been generated by a previous call to the API.
     *     @type \Google\GAX\RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\GAX\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\GAX\RetrySettings} for example usage.
     * }
     *
     * @return \Google\GAX\PagedListResponse
     */
    public function listTables($optionalArgs = [])
    {
        $PagedListResponse = $this->BigtableTableAdminClient->listTables($this->formattedInstance, $optionalArgs);
        return $PagedListResponse;
    }

    /**
     * Gets metadata information about the specified table.
     *
     * @param string $tableId
     *
     * @return \Google\Bigtable\Admin\V2\Table
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     */
    public function getTable($tableId)
    {
        $formattedTable = $this->tableName($tableId);
        return $this->BigtableTableAdminClient->getTable($formattedTable);
    }

    /**
     * Modify column family to perticular table.
     *
     * @param string $tableId
     *
     * @param string $cfName        Column family name.
     *
     * @param array $optionalArgs  {
     *                              Optional.
     *
     *     @type \Google\GAX\RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\GAX\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\GAX\RetrySettings} for example usage.
     * }
     *
     * @return \Google\Bigtable\Admin\V2\Table
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     */
    public function addColumnFamilies($tableId, $cfName, $optionalArgs = [])
    {
        $formattedTable = $this->tableName($tableId);
        $gc = new GcRule();
        $gc->setMaxNumVersions(3);

        $cf = new ColumnFamily();
        $cf->setGcRule($gc);

        $Modification = new Modification();
        $Modification->setId($cfName);
        $Modification->setCreate($cf);

        $Modifications    = [];
        $Modifications[0] = $Modification;

        $table = $this->BigtableTableAdminClient->modifyColumnFamilies($formattedTable, $Modifications, $optionalArgs);
        return $table;
    }

    /**
     * delete column family from perticular table.
     *
     * @param string $tableId
     *
     * @param string $cfName        Column family name.
     *
     * @param array $optionalArgs  {
     *                              Optional.
     *
     *     @type \Google\GAX\RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\GAX\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\GAX\RetrySettings} for example usage.
     * }
     *
     * @return \Google\Bigtable\Admin\V2\Table
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     */
    public function deleteColumnFamilies($tableId, $cfName, $optionalArgs = [])
    {
        $formattedTable = $this->tableName($tableId);
        $Modification = new Modification();
        $Modification->setId($cfName);
        $Modification->setDrop(true);
        $Modifications = [];
        $Modifications[] = $Modification;

        $table = $this->BigtableTableAdminClient->modifyColumnFamilies($formattedTable, $Modifications, $optionalArgs);
        return $table;
    }

    /**
     * Mutates multiple rows in a batch. Each individual row is mutated
     * atomically as in MutateRow, but the entire batch is not executed
     * atomically.
     *
     * @param string $tableId
     *
     * @param Entry[] $entries      The row keys and corresponding mutations to be applied in bulk.
     *                              Each entry is applied as an atomic mutation, but the entries may be
     *                              applied in arbitrary order (even between entries for the same row).
     *                              At least one entry must be specified, and in total the entries can
     *                              contain at most 100000 mutations.
     * @param array   $optionalArgs {
     *                              Optional.
     *
     *     @type int $timeoutMillis
     *          Timeout to use for this call.
     * }
     *
     * @return \Google\GAX\ServerStream
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     * @experimental
     */
    public function mutateRows($tableId, $entries, $optionalArgs = [])
    {
        $formattedTable = $this->tableName($tableId);
        $ServerStream = $this->BigtableClient->mutateRows($formattedTable, $entries, $optionalArgs);
        return $ServerStream;
    }

    /**
     * Mutates a row atomically. Cells already present in the row are left
     * unchanged unless explicitly changed by `mutation`.
     *
     * @param string $tableId
     *
     * @param string     $rowKey       The key of the row to which the mutation should be applied.
     *
     * @param Mutation[] $mutations    Changes to be atomically applied to the specified row. Entries are applied
     *                                 in order, meaning that earlier mutations can be masked by later ones.
     *                                 Must contain at least one entry and at most 100000.
     * @param array      $optionalArgs {
     *                                 Optional.
     *
     *     @type \Google\GAX\RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\GAX\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\GAX\RetrySettings} for example usage.
     * }
     *
     * @return \Google\Bigtable\V2\MutateRowResponse
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     */
    public function mutateRow($tableId, $rowKey, $mutations, $optionalArgs = [])
    {
        $formattedTable = $this->tableName($tableId);
        $MutateRowResponse = $this->BigtableClient->mutateRow($formattedTable, $rowKey, $mutations, $optionalArgs);
        return $MutateRowResponse;
    }

    /**
     * Set Mutation SetCell.
     *
     * @param array   $cell {
     *                 @type    string cf           Column Family name
     *                 @type    string qualifier    Qualifier name
     *                 @type    string value        value
     *                 @type    string timestamp    Timestamp in micros
     *
     * @return \Google\Bigtable\V2\Mutation
     */
    public function mutationCell($cell)
    {
        $Mutation_SetCell = new Mutation_SetCell();
        if (isset($cell['cf'])) {
            $Mutation_SetCell->setFamilyName($cell['cf']);
        }
        if (isset($cell['qualifier'])) {
            $Mutation_SetCell->setColumnQualifier($cell['qualifier']);
        }
        if (isset($cell['value'])) {
            $Mutation_SetCell->setValue($cell['value']);
        }
        if (isset($cell['timestamp'])) {
            $Mutation_SetCell->setTimestampMicros($cell['timestamp']);
        }

        $Mutation = new Mutation();
        $Mutation->setSetCell($Mutation_SetCell);
        return $Mutation;
    }

    /**
     * Set Mutate Rows Request.
     *
     * @param string $rowKey
     *
     * @param Mutation[] $mutations     array of \Google\Bigtable\V2\Mutation
     *
     * @return \Google\Bigtable\V2\MutateRowsRequest_Entry
     */
    public function mutateRowsRequest($rowKey, $mutations)
    {
        $MutateRowsRequest_Entry = new MutateRowsRequest_Entry();
        $MutateRowsRequest_Entry->setRowKey($rowKey);
        $MutateRowsRequest_Entry->setMutations($mutations);
        return $MutateRowsRequest_Entry;
    }

    /**
     * Read rows from table.
     *
     * @param string $tableId
     *
     * @param array  $optionalArgs {
     *                             Optional.
     *
     *     @type RowSet $rowKeys
     *          The row keys and/or ranges to read. If not specified, reads from all rows.
     *     @type RowFilter $filter
     *          The filter to apply to the contents of the specified row(s). If unset,
     *          reads the entirety of each row.
     *     @type int $rowsLimit
     *          The read will terminate after committing to N rows' worth of results. The
     *          default (zero) is to return all results.
     *     @type int $timeoutMillis
     *          Timeout to use for this call.
     * }
     *
     * @return \Google\Cloud\Bigtable\V2\FlatRow
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     */
    public function readRows($tableId, $optionalArgs = [])
    {
        $formattedTable = $this->tableName($tableId);
        $chunkFormatter = $this->BigtableClient->readRows($formattedTable, $optionalArgs);
        $rows           = [];
        foreach ($chunkFormatter->readAll() as $flatRow) {
            $rows[] = $flatRow;
        }
        return $rows;
    }

    /**
     * Returns a sample of row keys in the table. The returned row keys will
     * delimit contiguous sections of the table of approximately equal size,
     * which can be used to break up the data for distributed tasks like
     * mapreduces.
     *
     * @param string $tableId
     *
     * @param array  $optionalArgs {
     *                             Optional.
     *
     *     @type int $timeoutMillis
     *          Timeout to use for this call.
     * }
     *
     * @return \Google\GAX\ServerStream
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     * @experimental
     */
    public function sampleRowKeys($tableId, $optionalArgs = [])
    {
        $formattedTable = $this->tableName($tableId);
        $stream = $this->BigtableClient->sampleRowKeys($formattedTable, $optionalArgs);
        return $stream;
    }

    /**
     * Mutates a row atomically based on the output of a predicate Reader filter.
     *
     * @param string $tableId
     *
     * @param string $rowKey       The key of the row to which the conditional mutation should be applied.
     * @param array  $optionalArgs {
     *                             Optional.
     *
     *     @type RowFilter $predicateFilter
     *          The filter to be applied to the contents of the specified row. Depending
     *          on whether or not any results are yielded, either `true_mutations` or
     *          `false_mutations` will be executed. If unset, checks that the row contains
     *          any values at all.
     *     @type Mutation[] $trueMutations
     *          Changes to be atomically applied to the specified row if `predicate_filter`
     *          yields at least one cell when applied to `row_key`. Entries are applied in
     *          order, meaning that earlier mutations can be masked by later ones.
     *          Must contain at least one entry if `false_mutations` is empty, and at most
     *          100000.
     *     @type Mutation[] $falseMutations
     *          Changes to be atomically applied to the specified row if `predicate_filter`
     *          does not yield any cells when applied to `row_key`. Entries are applied in
     *          order, meaning that earlier mutations can be masked by later ones.
     *          Must contain at least one entry if `true_mutations` is empty, and at most
     *          100000.
     *     @type \Google\GAX\RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\GAX\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\GAX\RetrySettings} for example usage.
     * }
     *
     * @return \Google\Bigtable\V2\CheckAndMutateRowResponse
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     * @experimental
     */
    public function checkAndMutateRow($tableId, $rowKey, $optionalArgs = [])
    {
        $formattedTable = $this->tableName($tableId);
        $response = $this->BigtableClient->checkAndMutateRow($formattedTable, $rowKey, $optionalArgs);
        return $response;
    }

    /**
     * Modifies a row atomically. The method reads the latest existing timestamp
     * and value from the specified columns and writes a new entry based on
     * pre-defined read/modify/write rules. The new value for the timestamp is the
     * greater of the existing timestamp or the current server time. The method
     * returns the new contents of all modified cells.
     *
     * @param string $tableId
     *
     * @param string                $rowKey       The key of the row to which the read/modify/write
     *                                            rules should be applied.
     * @param ReadModifyWriteRule[] $rules        Rules specifying how the specified row's
     *                                            contents are to be transformed into writes.
     *                                            Entries are applied in order, meaning that earlier rules will
     *                                            affect the results of later ones.
     * @param array                 $optionalArgs {
     *                                            Optional.
     *
     *     @type \Google\GAX\RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\GAX\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\GAX\RetrySettings} for example usage.
     * }
     *
     * @return \Google\Bigtable\V2\ReadModifyWriteRowResponse
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     * @experimental
     */
    public function readModifyWriteRow($tableId, $rowKey, $rules, $optionalArgs = [])
    {
        $formattedTable = $this->tableName($tableId);
        $response = $this->BigtableClient->readModifyWriteRow($formattedTable, $rowKey, $rules, $optionalArgs);
        return $response;
    }
}
