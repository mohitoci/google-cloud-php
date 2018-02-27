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
use Google\Bigtable\V2\RowFilter;
use Google\Bigtable\V2\RowSet;
use Google\Bigtable\V2\RowRange;

/**
 *
 */
class ScanTest
{
	/**
	 * @var \Google\Cloud\Bigtable\Table
	 */
	private $table;

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
	 *     @param int $total_row
	 *     @param int $timeoutsec
 	 *     @param int $timeoutMillis Timeout to use for this call.
	 *
	 * @return array
	 */
	public function randomRead($tableId, $rowKey_pref, $cf, $option)
	{
		$total_row      = $option['totalRows']-1;
		$rowLimit		= $option['limit'];
		$readRowsTotal  = ['success' => 0, 'failure' => 0];

		$hdr_read  = hdr_init(1, 3600000, 3);

		$operation_start            = $this->currentMillies();
		$read_oprations_total_time  = 0;

		$startTime = date("h:i:s");
		echo "\nStart Time $startTime";
		$currentTimestemp = new DateTime($startTime);

		$endTime      = date("h:i:s", time()+$option['timeoutSec']);//sec
		$endTimestemp = new DateTime($endTime);
		echo "\nProcess will terminate after $endTime";
		echo "\nPlease wait ...";
		while ($currentTimestemp < $endTimestemp) {
			$random = mt_rand(0, $total_row);
			$randomRowKey = sprintf($rowKey_pref.'%07d', $random);

			$RowRange = new RowRange();
			$RowRange->setStartKeyClosed($randomRowKey);

			//Row set
			$RowSet = new RowSet();
			$RowSet->setRowRanges([$RowRange]);

			//Row Filter
			$RowFilter = new RowFilter();
			$RowFilter->setCellsPerRowLimitFilter(1);

			$optionalArg['rows'] = $RowSet;
			$optionalArg['filter'] = $RowFilter;
			$optionalArg['rowsLimit'] = $rowLimit;
			$startAt = $this->currentMillies();
			$res = $this->table->readRows($tableId, $optionalArg);
			$time_elapsed = $this->currentMillies() - $startAt;
			if (count($res)) {
				$readRowsTotal['success']++;
			} else {
				$readRowsTotal['failure']++;
			}
			$read_oprations_total_time += $time_elapsed;
			hdr_record_value($hdr_read, $time_elapsed);
			$currentTimestemp = new DateTime(date("h:i:s"));
		}
		echo "\noperation complete\n";
		$total_runtime = $this->currentMillies() - $operation_start;
		//Read operations
		$min_read       = hdr_min($hdr_read);
		$max_read       = hdr_max($hdr_read);
		$total_read     = $readRowsTotal['success'] + $readRowsTotal['failure'];
		$totalReadTimeSec = $read_oprations_total_time/1000;
		$readThroughput = round($total_read/$totalReadTimeSec, 4);
		$statisticsData = [
			'operation_name'     => 'Read',
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
		return $statisticsData;
	}

	/**
	 * current milli sec
	 *
	 * @return int
	 */
	public function currentMillies()
	{
		return round(microtime(true)*1000);
	}
}

foreach ($argv as $val) {
	if (strpos($val, 'help') !== false) {
		$txt = "--projectId\t projectId \n\n--instanceId\t instanceId \n\n--tableId\t table name to perform operations \n\n--totalRows\t Total no. of rows to find random key \n\n--timeoutMinute\t random read write rows load till defined timeoutMinute \n\n--limit\t\t read rows at a time \n\nEx. php ScanTest.php projectId=grass-clump-479 instanceId=php-perf tableId=scantest totalRows=1000000 timeoutMinute=30 limit=100\n\n";
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
		}
	} else if (strpos($val, 'timeoutMinute') !== false) {
		$val = explode('=', $val);
		if (count($val) > 1 && is_int((int) $val[1])) {
			$timeoutMinute = (int) $val[1];
		}
	} else if (strpos($val, 'limit') !== false) {
		$val = explode('=', $val);
		if (count($val) > 1 && is_int((int) $val[1])) {
			$limit = (int) $val[1];
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

if (!isset($timeoutMinute)) {
	exit("timeoutMinute is missing\n");
}

if (!isset($limit)) {
	exit("limit is missing\n");
}

$args = ['projectId' => $projectId, 'instanceId' =>$instanceId];
$ScanTest = new ScanTest($args);

/*********  Read rows *************/
echo "\nScan test operations for $limit rowKeys";
$timeoutSec = $timeoutMinute * 60;
$options    = ['totalRows' => $totalRows, 'timeoutSec' => $timeoutSec, 'limit' => $limit];
$rowKey_pref  = 'perf';
$columnFamily = 'cf';
$statisticsData = $ScanTest->randomRead($tableId, $rowKey_pref, $columnFamily, $options);

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

$filepath = 'scan_test_for_'.$limit.'_At_'.date("m_d_Y_h_i_s").'.csv';
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
fputcsv($fp, array_values($statisticsData));
fclose($fp);

echo "\nFile generated ".$filepath;
echo "\n----------------------------------------------------------------\n";

/*********  Printing stats *************/
foreach($header as $key => $val){
	echo "$val : ".$statisticsData[$key];
	echo "\n";
}
echo "\n";
