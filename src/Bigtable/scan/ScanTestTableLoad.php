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

use Google\Cloud\Bigtable\src\BigtableTable;
use Google\Bigtable\V2\RowSet;

/**
 * 
 */
class ScanTestTableLoad
{
	private $BigtableTable;
	private $randomValues;
	private $randomTotal = 1000;

	/**
     * Constructor.
     * @param array $args {
     *
     *     @param string $projectId
     *
     *     @param string $instanceId
     */
	function __construct($args)
	{
		$this->BigtableTable = new BigtableTable($args);
		$length              = 100;
		for ($i = 1; $i <= $this->randomTotal; $i++) {
			$this->randomValues[$i] = substr(str_shuffle(str_repeat($x = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', ceil($length/strlen($x)))), 1, $length);
		}
	}

	/**
     * Create Table
     *
     * @param string $tableId
     *
     * @param string $columnFamily
     */
	public function createTable($tableId, $columnFamily)
	{
		try{
			$this->BigtableTable->getTable($tableId);
		}
		catch(Exception $e){
			$error = json_decode($e->getMessage() );
			if($error->status == 'NOT_FOUND'){
				$this->BigtableTable->createTableWithColumnFamily($tableId, $columnFamily);
			}
		}
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
					$MutationArray[$i] = $this->BigtableTable->mutationCell($cell);
				}
				// setMutations
				$entries[$index] = $this->BigtableTable->mutateRowsRequest($rowKey, $MutationArray);
				$index++;
			}

			$startTime    = $this->milliSec();
			$ServerStream = $this->BigtableTable->mutateRows($tableId, $entries, $optionalArgs);
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
		$throughput    = round($total/$totalSec, 4);
		$statesticData = [
			'operation_name'     => "Load ($batch_size Batch)",
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
}

foreach ($argv as $val) {
	if (strpos($val, 'help') !== false) {
		$txt = "--projectId\t projectId \n\n--instanceId\t instanceId \n\n--tableId\t table name to perform operations \n\n--totalRows\t Total no. of rows to inserting \t totalRows >= batchSize \n\n--batchSize\t Defines that how many rows mutate at a time \t batchSize is > 0 and <10000 \n\n--timeoutMillis\t timeoutMillis for mutate rows \n\nEx. php ScanTestTableLoad.php projectId=grass-clump-479 instanceId=php-perf tableId=scantest totalRows=10000000 batchSize=1000 \nNote. timeoutMillis are optional \n\n";
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
	} else if (strpos($val, 'tableId') !== false) {
		$val = explode('=', $val);
		if (count($val) > 1) {
			$tableId = $val[1];
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
	}
}

if (!isset($projectId)) {
	exit("projectId is missing\n");
}
if (!isset($instanceId)) {
	exit("instanceId is missing\n");
}
if (!isset($tableId)) {
	exit("tableId is missing\n");
}

if (!isset($totalRows)) {
	exit("totalRows is missing\n");
}

if (!isset($batchSize)) {
	exit("batchSize is missing\n");
}

$args = ['projectId' => $projectId, 'instanceId' =>$instanceId];
$scanTest = new ScanTestTableLoad($args);

/*********  Creating table *************/
echo "Creating table  $tableId \n";
$columnFamily = 'cf';
$scanTest->createTable($tableId, $columnFamily);

/*********  Loading rows *************/
$rowKey_pref  = 'perf';
$options = ['total_row' => $totalRows, 'batch_size' => $batchSize];
if (isset($timeoutMillis)) {
	$options['timeoutMillis'] = $timeoutMillis;
}
echo "\n$totalRows rows loading ... \n";
$reportData = $scanTest->loadRecord($tableId, $rowKey_pref, $columnFamily, $options);

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

$filepath = 'scantest_table_load_At_'.date("m_d_Y_h_i_s").'.csv';

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
fputcsv($fp, array_values($reportData));
fclose($fp);

echo "\nFile generated ".$filepath;
echo "\n----------------------------------------------------------------\n";

/*********  Printing stats *************/
foreach($header as $key => $val){
	echo "$val : ".$reportData[$key];
	echo "\n";
}
echo "\n";
