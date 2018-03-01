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

require_once __DIR__.'/../vendor/autoload.php';

use Google\Cloud\Bigtable\Table;
use Google\Bigtable\V2\RowSet;

/**
 * Performance test for load rows and random read write
 * 
 */
class PerformanceTest
{
	/**
	 * @var \Google\Cloud\Bigtable\Table
	 */
	private $table;

	/**
     * @var array
     */
	private $randomValues;

	/**
     * @var integer
     */
	private $randomTotal = 1000;

	/**
     * Config table Client.
     *
	 * Create random value 100 byte string and store into randomValues array
	 * 
     * @param array $config {
     *      Configuration Options.
     *
     *     @param string $projectId
     *
     *     @param string $instanceId
     */
	function __construct($config)
	{
		$this->table = new table($config);
		$length              = 100;
		for ($i = 1; $i <= $this->randomTotal; $i++) {
			$this->randomValues[$i] = substr(str_shuffle(str_repeat($x = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', ceil($length/strlen($x)))), 1, $length);
		}
	}

	/**
     * Create table perf{8 Random string}
     *
     * @param string $columnFamily
     */
	public function createTable($columnFamily)
	{
		$length = 8;
		$tableId = "perf".substr(str_shuffle(str_repeat($x = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', ceil($length/strlen($x)))), 1, $length);
		echo "Creating table  $tableId\n";
		try{
			$this->table->getTable($tableId);
		}
		catch(Exception $e){
			$error = json_decode($e->getMessage() );
			if($error->status == 'NOT_FOUND'){
				$this->table->createTableWithColumnFamily($tableId, $columnFamily);
			}
		}
		return $tableId;
	}

	/**
     * Delete Table
     *
     * @param string $tableId
     */
	public function deleteTable($tableId)
	{
		$this->table->deleteTable($tableId);
	}

	/**
	 * current milli sec
	 *
	 * @return int
	 */
	public function milliSec()
	{
		return round(microtime(true)*1000);
	}

	/**
	 * loadRecord for mutateRows in table
	 *
	 * @param string $tableId
	 * 
	 * @param string $rowKey_pref   ex. perf
	 * 
	 * @param string $columnFamily	column family name
	 * 
	 * @param array  optionalArgs{
	 *     @param int $total_row
	 *     @param int $batch_size
	 *     @param int $timeoutMillis Timeout to use for this call.
	 *
	 * @return array
	 */
	public function loadRecord($tableId, $rowKey_pref, $columnFamily, $optionalArgs = [])
	{
		$total_row  = $optionalArgs['total_row'];
		$batch_size = $optionalArgs['batch_size'];

		if ($total_row < $batch_size) {
			throw new ValidationException('Please set total row (total_row) >= '.$batch_size);
		}
		$interations = $total_row/$batch_size;

		$hdr = hdr_init(1, 3600000, 3);

		$index            = 0;
		$success          = 0;
		$failure          = 0;
		$total_time_elapsed = 0;
		//$processStartTime = $this->milliSec();
		for ($k = 0; $k < $interations; $k++) {
			$entries = [];
			for ($j = 0; $j < $batch_size; $j++) {
				$rowKey        = sprintf($rowKey_pref.'%07d', $index);
				$MutationArray = [];
				for ($i = 0; $i < 10; $i++) {
					$value             = $this->randomValues[mt_rand(1, $this->randomTotal)];
					$utc_str           = gmdate("M d Y H:i:s", time());
					$utc               = strtotime($utc_str);
					$cell['cf']        = $columnFamily;
					$cell['qualifier'] = 'field'.$i;
					$cell['value']     = $value;
					$cell['timestamp'] = $utc*1000;
					$MutationArray[$i] = $this->table->mutationCell($cell);
				}
				// setMutations
				$entries[$index] = $this->table->mutateRowsRequest($rowKey, $MutationArray);
				$index++;
			}

			$startTime    = $this->milliSec();
			$ServerStream = $this->table->mutateRows($tableId, $entries, $optionalArgs);
			$current      = $ServerStream->readAll()->current();
			$Entries      = $current->getEntries();
			foreach ($Entries->getIterator() as $Iterator) {
				$status = $Iterator->getStatus();
				$code   = $status->getCode();
				if ($code == 0) {
					$success++;
				} else if ($code == 1) {
					$failure++;
				}
			}
			$time_elapsed = $this->milliSec() - $startTime;
			hdr_record_value($hdr, $time_elapsed);
			$total_time_elapsed += $time_elapsed;
			// echo "\nProcess time for mutateRows $index is $time_elapsed";
		}
		//$total_time_elapsed = $this->milliSec() - $processStartTime;
		echo "\nTotal time take for loading rows is $total_time_elapsed (milli sec)";

		$min           = hdr_min($hdr);
		$max           = hdr_max($hdr);
		$total         = $index;
		$totalSec      = $total_time_elapsed / 1000;
		$throughput    = round($total/$totalSec, 2);
		$statesticData = [
			'operation_name'     => 'Data Load',
			'run_time'           => $total_time_elapsed,
			'mix_latency'        => $max/100,
			'min_latency'        => $min/100,
			'oprations'          => $total,
			'throughput'         => $throughput,
			'p50_latency'        => hdr_value_at_percentile($hdr, 50),
			'p75_latency'        => hdr_value_at_percentile($hdr, 75),
			'p90_latency'        => hdr_value_at_percentile($hdr, 90),
			'p95_latency'        => hdr_value_at_percentile($hdr, 95),
			'p99_latency'        => hdr_value_at_percentile($hdr, 99),
			'p99.99_latency'     => hdr_value_at_percentile($hdr, 99.99),
			'success_operations' => $success,
			'failed_operations'  => $failure
		];
		return $statesticData;
	}

	/**
	 * random read write row
	 *
	 * @param string $tableId
	 *
	 * @param string $rowKey_pref   ex. perf
	 *
	 * @param string $cf   			column family name
	 *
	 * @param array  option{
	 * 
	 *     @param int $total_row
	 *     @param int $timeoutsec
	 *
	 * @return array
	 */
	public function randomReadWrite($tableId, $rowKey_pref, $cf, $option)
	{
		$total_row      = $option['total_row']-1;
		$readRowsTotal  = ['success' => 0, 'failure' => 0];
		$writeRowsTotal = ['success' => 0, 'failure' => 0];

		$hdr_read  = hdr_init(1, 3600000, 3);
		$hdr_write = hdr_init(1, 3600000, 3);

		$operation_start            = $this->milliSec();
		$read_oprations_total_time  = 0;
		$write_oprations_total_time = 0;

		$startTime = date("m/d/y h:i:s");
		echo "\nRandom read write process start Time $startTime";
		$currentTimestemp = new DateTime($startTime);

		$endTime      = date("m/d/y h:i:s", time()+$option['timeoutsec']);//sec
		$endTimestemp = new DateTime($endTime);
		echo "\nProcess will terminate after $endTime";
		echo "\nPlease wait ...";
		$i = 0;
		while ($currentTimestemp < $endTimestemp) {
			$random       = mt_rand(0, $total_row);
			$randomRowKey = sprintf($rowKey_pref.'%07d', $random);

			if ($i%2 == 0) {
				//Row set
				$RowSet = new RowSet();
				$RowSet->setRowKeys([$randomRowKey]);
				$optionalArg['rows'] = $RowSet;

				$startAt = $this->milliSec();
				$res = $this->table->readRows($tableId, $optionalArg);
				$time_elapsed = $this->milliSec() - $startAt;
				if (count($res)) {
					$readRowsTotal['success']++;
				} else {
					$readRowsTotal['failure']++;
				}
				$read_oprations_total_time += $time_elapsed;
				hdr_record_value($hdr_read, $time_elapsed);
			} else {
				$value             = $this->randomValues[mt_rand(1, $this->randomTotal)];
				$cell['cf']        = $cf;//Specify column name, without column familly not updating row
				$cell['value']     = $value;
				$cell['qualifier'] = 'field0';//Specify qualifier (optional)

				$mutationCell = $this->table->mutationCell($cell);
				$startAt = $this->milliSec();
				$this->table->mutateRow($tableId, $randomRowKey, [$mutationCell]);
				$time_elapsed = $this->milliSec() - $startAt;
				$writeRowsTotal['success']++;

				$write_oprations_total_time += $time_elapsed;
				hdr_record_value($hdr_write, $time_elapsed);
			}
			$i++;
			$currentTimestemp = new DateTime(date("h:i:s"));
		}
		$total_runtime = $this->milliSec() - $operation_start;
		
		//Read operations
		$min_read       = hdr_min($hdr_read);
		$max_read       = hdr_max($hdr_read);
		$total_read     = $readRowsTotal['success'] + $readRowsTotal['failure'];
		$totalReadTimeSec = $read_oprations_total_time/1000;
		$readThroughput = round($total_read/$totalReadTimeSec, 2);
		$readOperations = [
			'operation_name'     => 'Random Read',
			'run_time'           => $read_oprations_total_time,
			'mix_latency'        => $max_read/100,
			'min_latency'        => $min_read/100,
			'oprations'          => $total_read,
			'throughput'         => $readThroughput,
			'p50_latency'        => hdr_value_at_percentile($hdr_read, 50),
			'p75_latency'        => hdr_value_at_percentile($hdr_read, 75),
			'p90_latency'        => hdr_value_at_percentile($hdr_read, 90),
			'p95_latency'        => hdr_value_at_percentile($hdr_read, 95),
			'p99_latency'        => hdr_value_at_percentile($hdr_read, 99),
			'p99.99_latency'     => hdr_value_at_percentile($hdr_read, 99.99),
			'success_operations' => $readRowsTotal['success'],
			'failed_operations'  => $readRowsTotal['failure']
		];

		//Write Operations
		$min_write       = hdr_min($hdr_write);
		$max_write       = hdr_max($hdr_write);
		$total_write     = $writeRowsTotal['success'] + $writeRowsTotal['failure'];
		$totalWriteTimeSec = $write_oprations_total_time/1000;
		$writeThroughput = round($total_write/$totalWriteTimeSec, 2);
		$writeOperations = [
			'operation_name'     => 'Random Write',
			'run_time'           => $write_oprations_total_time,
			'mix_latency'        => $max_write/100,
			'min_latency'        => $min_write/100,
			'oprations'          => $total_write,
			'throughput'         => $writeThroughput,
			'p50_latency'        => hdr_value_at_percentile($hdr_write, 50),
			'p75_latency'        => hdr_value_at_percentile($hdr_write, 75),
			'p90_latency'        => hdr_value_at_percentile($hdr_write, 90),
			'p95_latency'        => hdr_value_at_percentile($hdr_write, 95),
			'p99_latency'        => hdr_value_at_percentile($hdr_write, 99),
			'p99.99_latency'     => hdr_value_at_percentile($hdr_write, 99.99),
			'success_operations' => $writeRowsTotal['success'],
			'failed_operations'  => $writeRowsTotal['failure']
		];
		return (['readOperations' => $readOperations, 'writeOperations' => $writeOperations]);
	}
}

foreach ($argv as $val) {
	if (strpos($val, 'help') !== false) {
		$txt = "--projectId\t projectId \n\n--instanceId\t instanceId \n\n--totalRows\t Total no. of rows to inserting \t totalRows >= batchSize \n\n--batchSize\t Defines that how many rows mutate at a time \t batchSize is > 0 and <10000 \n\n--timeoutMinute\t random read write rows load till defined timeoutMinute \n\n--timeoutMillis\t timeoutMillis for mutate rows \n\nEx. php PerformanceTest.php projectId=grass-clump-479 instanceId=php-perf totalRows=10000 batchSize=1000 timeoutMinute=30 \nNote. timeoutMillis are optional \n\n";
		exit($txt);
	} else if (strpos($val, 'projectId') !== false) {
		$val = explode('=', $val);
		if (count($val) > 1) {
			$projectId = $val[1];
		}
	} else if (strpos($val, 'instanceId') !== false) {
		$val = explode('=', $val);
		if (count($val) > 1) {
			$instanceId = $val[1];
		}
	} else if (strpos($val, 'totalRows') !== false) {
		$val = explode('=', $val);
		if (count($val) > 1 && is_int((int) $val[1])) {
			$totalRows = (int) $val[1];
		} else {
			exit("totalRows is integer and >= batchSize\n");
		}
	} else if (strpos($val, 'batchSize') !== false) {
		$val = explode('=', $val);
		if (count($val) > 1 && is_int((int) $val[1])) {
			$batchSize = (int) $val[1];
		} else {
			exit("batchSize is integer and > 0\n");
		}
	} else if (strpos($val, 'timeoutMillis') !== false) {
		$val = explode('=', $val);
		if (count($val) > 1 && is_int((int) $val[1])) {
			$timeoutMillis = (int) $val[1];
		}
	} else if (strpos($val, 'timeoutMinute') !== false) {
		$val = explode('=', $val);
		if (count($val) >= 1 && is_int((int) $val[1])) {
			$minute = (int) $val[1];
		} else {
			exit("timeoutMinute is >= 1\n");
		}
	}
}

if (!isset($projectId)) {
	exit("projectId is missing\n");
}
if (!isset($instanceId)) {
	exit("instanceId is missing\n");
}

if (!isset($totalRows)) {
	exit("totalRows is missing\n");
}

if (!isset($batchSize)) {
	exit("batchSize is missing\n");
}

if (!isset($minute)) {
	exit("timeoutMinute is missing\n");
}

$args = ['projectId' => $projectId, 'instanceId' =>$instanceId];
$PerformanceTest = new PerformanceTest($args);

/*********  Creating table *************/
$columnFamily = 'cf';
$tableId = $PerformanceTest->createTable($columnFamily);

/*********  Loading rows *************/
$rowKey_pref  = 'perf';
$options = ['total_row' => $totalRows, 'batch_size' => $batchSize];
if (isset($timeoutMillis)) {
	$options['timeoutMillis'] = $timeoutMillis;
}
echo "\n$totalRows rows loading ... \n";
$inserted = $PerformanceTest->loadRecord($tableId, $rowKey_pref, $columnFamily, $options);

/*********  Random read write *************/
echo "\nLoad phase completed starting random read write phase";
$timeoutsec      = $minute *60;//sec
$options         = ['total_row' => $totalRows, 'timeoutsec' => $timeoutsec];
$randomReadWrite = $PerformanceTest->randomReadWrite($tableId, $rowKey_pref, $columnFamily, $options);
echo "\nRandom read write phase completed\n";

/*********  Write csv file *************/
$info = array(
	'Platform,Linux',
	'PHP,v7.0',
	'Bigtable,v2.0',
	'Start Time,'.gmdate("D M d Y H:i:s e"),
	'',
	'NOTE: All values are in milliseconds',
	'',
);

$filepath = 'reports_latency_test_At_'.date("m_d_Y_h_i_s").'.csv';

$fp       = fopen($filepath, "w");
foreach ($info as $line) {
	$val = explode(",", $line);
	fputcsv($fp, $val);
}

$header = [
	'operation_name'     => 'Operation Name',
	'run_time'           => 'Run Time',
	'mix_latency'        => 'Max Latency',
	'min_latency'        => 'Min Latency',
	'oprations'          => 'Operations',
	'throughput'         => 'Throughput',
	'p50_latency'        => 'p50 Latency',
	'p75_latency'        => 'p75 Latency',
	'p90_latency'        => 'p90 Latency',
	'p95_latency'        => 'p95 Latency',
	'p99_latency'        => 'p99 Latency',
	'p99.99_latency'     => 'p99.99 Latency',
	'success_operations' => 'Success Operations',
	'failed_operations'  => 'Failed Operations'
];
fputcsv($fp, array_values($header));
fputcsv($fp, array_values($inserted));
fputcsv($fp, array_values($randomReadWrite['readOperations']));
fputcsv($fp, array_values($randomReadWrite['writeOperations']));
fclose($fp);

echo "\nFile generated ".$filepath;
echo "\n----------------------------------------------------------------\n";

/*********  Printing stats *************/
foreach($header as $key => $val){
	echo "$val : ".$inserted[$key];
	echo "\n";
}
echo "\n";

foreach($header as $key => $val){
	echo "$val : ".$randomReadWrite['readOperations'][$key];
	echo "\n";
}
echo "\n";

foreach($header as $key => $val){
	echo "$val : ".$randomReadWrite['writeOperations'][$key];
	echo "\n";
}

/*********  Delete table *************/
echo "\nDeleting table : $tableId";
$PerformanceTest->deleteTable($tableId);
echo "\n";
