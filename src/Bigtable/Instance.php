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

namespace Google\Cloud\Bigtable;

use Google\Cloud\Bigtable\Admin\V2\BigtableInstanceAdminClient;
use Google\Bigtable\Admin\V2\Cluster;
use Google\Bigtable\Admin\V2\Instance_Type;
use Google\Bigtable\Admin\V2\StorageType;
use Google\Protobuf\FieldMask;
use Google\Protobuf\Internal\MapField;
use Google\Protobuf\Internal\GPBType;

/**
 * Service for creating, configuring, and deleting Cloud Bigtable Instances and
 * Clusters. Provides access to the Instance and Cluster schemas.
 *
 * This class provides the ability to make remote calls to the backing service through method
 * calls that map to API methods. Sample code to get started:
 *
 * Example:
 * ```
 * use Google\Cloud\Bigtable;
 *
 * $config = array('projectId' => '[PROJECT]', 'instanceId' => '[INSTANCE]')
 * $instance = new instance($config);
 *
 * $instanceId = 'php-test';
 * $locationId = 'us-east1-b';
 * $clusterId = 'cluster';
 * $operationResponse = $instance->createInsatnce($instanceId, $locationId, $clusterId);
 * $operationName = $operationResponse->getName();
 * ```
 *
 * Many parameters require resource names to be formatted in a particular way. To assist
 * with these names, this class includes a format method for each type of name, and additionally
 * a parseName method to extract the individual identifiers contained within formatted names
 * that are returned by the API.
 */
class Instance
{
    /**
     * @var Admin\V2\BigtableInstanceAdminClient
     */
    private $instanceAdminClient;

    /**
     * @var string
     */
    private $projectId;

    /**
     * @var string
     */
    private $instanceId;

    /**
     * Constructor.
     * @param array $args {
     *
     *     @param string $projectId
     *
     *     @param string $instanceId
     *
     */
    public function __construct($args)
    {
        $this->projectId = $args['projectId'];
        $this->instanceId = $args['instanceId'];
        $this->instanceAdminClient = new BigtableInstanceAdminClient();
    }
    
    /**
     * Formats a string containing the fully-qualified path to represent
     * a project resource.
     *
     * @return string The formatted project resource.
     */
    private function projectName()
    {
        $formattedParent = BigtableInstanceAdminClient::projectName($this->projectId);
        return $formattedParent;
    }

    /**
     * Formats a string containing the fully-qualified path to represent
     * a instance resource.
     *
     * @param string $instanceId    Optional.
     *
     * @return string The formatted instance resource.
     */
    private function instanceName($instanceId = '')
    {
        if ($instanceId == '') {
            $instanceId = $this->instanceId;
        }
        $formattedParent = BigtableInstanceAdminClient::instanceName($this->projectId, $instanceId);
        return $formattedParent;
    }

    /**
     * Formats a string containing the fully-qualified path to represent
     * a location resource.
     *
     * @param string $locationId
     *
     * @return string The formatted location resource.
     */
    private function locationName($locationId)
    {
        return BigtableInstanceAdminClient::locationName($this->projectId, $locationId);
    }

    /**
     * Formats a string containing the fully-qualified path to represent
     * a cluster resource.
     *
     * @param string $clusterId
     *
     * @return string The formatted cluster resource.
     */
    private function clusterName($clusterId)
    {
        return BigtableInstanceAdminClient::clusterName($this->projectId, $this->instanceId, $clusterId);
    }

    /**
     * Create an instance within a project.
     *
     * Example:
     * ```
     * $instanceId = 'php-test';
     * $locationId = 'us-east1-b';
     * $clusterId = 'cluster';
     * $operationResponse = $instance->createInsatnce($instanceId, $locationId, $clusterId);
     * $operationName = $operationResponse->getName();
     * ```
     *
     * @param string   $instanceId   The ID to be used when referring to the new instance within its project,
     *                               e.g., just `myinstance` rather than
     *                               `projects/myproject/instances/myinstance`.
     *
     * @param string   $locationId   The unique location id.
     *
     * @param string   $clusterId    The unique id of the cluster to be create.
     *
     * @param array    $optionalArgs {
     *                               Optional.
     *
     *     @type \Google\GAX\RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\GAX\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\GAX\RetrySettings} for example usage.
     * }
     *
     * @return \Google\GAX\OperationResponse
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     */
    public function createInstance($instanceId, $locationId, $clusterId, $optionalArgs = [])
    {
        $parent = $this->projectName();
        $formattedLocation = $this->locationName($locationId);

        $instance = new \Google\Cloud\Bigtable\Admin\V2\Instance();
        $instance->setDisplayName($instanceId);
        $instance->setType(Instance_Type::PRODUCTION);

        $clusters = new Cluster();
        $clusters->setName($clusterId);
        $clusters->setDefaultStorageType(2);
        $clusters->setLocation($formattedLocation);
        $MapField = new MapField(GPBType::STRING, GPBType::MESSAGE, Cluster::class);
        $MapField[$clusterId] = $clusters;

        $instanceId = str_replace(' ', '-', $instanceId);
        $Instance = $this->instanceAdminClient->createInstance($parent, $instanceId, $instance, $MapField, $optionalArgs);
        return $Instance;
    }

    /**
     * Gets information about an instance.
     *
     * Example:
     * ```
     * $instanceId = 'php-test';
     * $operationResponse = $instance->getInstance($instanceId);
     * ```
     *
     * @param string $instanceId   The unique name of the requested instance.
     * @param array  $optionalArgs {
     *                             Optional.
     *
     *     @type RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\ApiCore\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\ApiCore\RetrySettings} for example usage.
     * }
     *
     * @return \Google\Cloud\Bigtable\Admin\V2\Instance
     *
     * @throws ApiException if the remote call fails
     * @experimental
     */
    public function getInstance($instanceId, $optionalArgs = [])
    {
        $formattedName = $this->instanceName($instanceId);
        $response = $this->instanceAdminClient->getInstance($formattedName);
        return $response;
    }

    /**
     * Updates an instance within a project.
     *
     * Example:
     * ```
     * $displayName = 'Php Test';
     * $type = \Google\Bigtable\Admin\V2\Instance_Type::PRODUCTION;
     * $labels = [];
     * $response = $instance->updateInstance($displayName, $type, $labels);
     * ```
     *
     * @param string $displayName  The descriptive name for this instance as it appears in UIs.
     *                             Can be changed at any time, but should be kept globally unique
     *                             to avoid confusion.
     * @param int    $type         The type of the instance. Defaults to `PRODUCTION`.
     *                             For allowed values, use constants defined on
     *                             {@see \Google\Bigtable\Admin\V2\Instance_Type}
     * @param array  $labels      Labels are a flexible and lightweight mechanism for organizing cloud
     *                            resources into groups that reflect a customer's organizational needs and
     *                            deployment strategies. They can be used to filter resources and aggregate
     *                            metrics.
     *
     * * Label keys must be between 1 and 63 characters long and must conform to
     *   the regular expression: `[\p{Ll}\p{Lo}][\p{Ll}\p{Lo}\p{N}_-]{0,62}`.
     * * Label values must be between 0 and 63 characters long and must conform to
     *   the regular expression: `[\p{Ll}\p{Lo}\p{N}_-]{0,63}`.
     * * No more than 64 labels can be associated with a given resource.
     * * Keys and values must both be under 128 bytes.
     *
     * @param array  $optionalArgs {
     *                             Optional.
     *
     *     @type int $state
     *          (`OutputOnly`)
     *          The current state of the instance.
     *          For allowed values, use constants defined on {@see \Google\Bigtable\Admin\V2\Instance_State}
     *     @type \Google\GAX\RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\GAX\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\GAX\RetrySettings} for example usage.
     * }
     *
     * @return \Google\Bigtable\Admin\V2\Instance
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     */
    public function updateInstance($displayName, $type, $labels, $optionalArgs = [])
    {
        $name = $this->instanceName();
        $Instance = $this->instanceAdminClient->updateInstance($name, $displayName, $type, $labels, $optionalArgs);
        return $Instance;
    }

    /**
     * Lists information about instances in a project.
     *
     * Example:
     * ```
     * $response = $instance->listInstances();
     * ```
     *
     * @param array  $optionalArgs {
     *                             Optional.
     *
     *     @type string $pageToken
     *          The value of `next_page_token` returned by a previous call.
     *     @type \Google\GAX\RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\GAX\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\GAX\RetrySettings} for example usage.
     * }
     *
     * @return \Google\Bigtable\Admin\V2\ListInstancesResponse
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     */
    public function listInstances($optionalArgs = [])
    {
        $parent = $this->projectName();
        $ListInstances = $this->instanceAdminClient->listInstances($parent, $optionalArgs);
        return $ListInstances;
    }

    /**
     * Delete an instance from a project.
     *
     * Example:
     * ```
     * $instanceId = 'foobar';
     * $instance->deleteInstance($instanceId);
     * ```
     *
     * @param string $instanceId   The unique name of the requested instance.
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
     * @return \Google\Protobuf\GPBEmpty
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     */
    public function deleteInstance($instanceId, $optionalArgs = [])
    {
        $formattedParent = $this->instanceName($instanceId);
        $response = $this->instanceAdminClient->deleteInstance($formattedParent, $optionalArgs);
        return $response;
    }

    /**
     * Creates a cluster within an instance.
     *
     * Example:
     * ```
     * $locationId = 'us-east1-b';
     * $clusterId = 'cluster';
     * $operationResponse = $instance->createCluster($locationId, $clusterId);
     * $operationName = $operationResponse->getName();
     * ```
     *
     * @param string  $locationId
     *
     * @param string  $clusterId    The ID to be used when referring to the new cluster within its instance,
     *                              e.g., just `mycluster` rather than
     *                              `projects/myproject/instances/myinstance/clusters/mycluster`.
     *
     * @param array   $optionalArgs {
     *                              Optional.
     *
     *     @type RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\ApiCore\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\ApiCore\RetrySettings} for example usage.
     * }
     *
     * @return \Google\ApiCore\OperationResponse
     *
     * @throws ApiException if the remote call fails
     */
    public function createCluster($locationId, $clusterId, $optionalArgs = [])
    {
        $formattedParent = $this->instanceName();
        $cluster = new Cluster();
        $cluster->setLocation($this->locationName($locationId));
        $cluster->setDefaultStorageType(StorageType::HDD);

        $clusterId = str_replace(' ', '-', $clusterId);
        $operationResponse = $this->instanceAdminClient->createCluster($formattedParent, $clusterId, $cluster, $optionalArgs);
        return $operationResponse;
    }

    /**
     * Gets information about a cluster.
     *
     * Example:
     * ```
     * $clusterId = 'cluster';
     * $response = $instance->getCluster($clusterId);
     * ```
     *
     * @param string $clusterId
     *
     * @param array  $optionalArgs {
     *                             Optional.
     *
     *     @type RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\ApiCore\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\ApiCore\RetrySettings} for example usage.
     * }
     *
     * @return \Google\Cloud\Bigtable\Admin\V2\Cluster
     *
     * @throws ApiException if the remote call fails
     */
    public function getCluster($clusterId, $optionalArgs = [])
    {
        $formattedName = $this->clusterName($clusterId);
        $cluster = $this->instanceAdminClient->getCluster($formattedName, $optionalArgs);
        return $cluster;
    }

    /**
     * Lists information about clusters in an instance.
     *
     * Example:
     * ```
     * $instanceid = '-';
     * $response = $instance->listClusters($instanceid);
     * 
     * OR
     * 
     * $response = $instance->listClusters();
     * ```
     *
     * @param string $instanceid      {
     *                              Optional.   instanceid
     *                             Use `<instance> = '-'` to list Clusters for all Instances in a project,
     *                             e.g., `projects/myproject/instances/-`.
     * @param array  $optionalArgs {
     *                             Optional.
     *
     *     @type string $pageToken
     *          The value of `next_page_token` returned by a previous call.
     *     @type RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\ApiCore\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\ApiCore\RetrySettings} for example usage.
     * }
     *
     * @return \Google\Cloud\Bigtable\Admin\V2\ListClustersResponse
     *
     * @throws ApiException if the remote call fails
     */
    public function listClusters($instanceid = '', $optionalArgs = [])
    {
        $instanceName = $this->instanceName($instanceid);
        $clusters = $this->instanceAdminClient->listClusters($instanceName, $optionalArgs);
        return $clusters;
    }

    /**
     * Updates a cluster within an instance.
     *
     * Example:
     * ```
     * $clusterId = "cluster";
     * $locationId = "us-east1-b";
     * $serveNodes = 0;
     * $response = $instance->updateCluster($clusterId, $locationId, $serveNodes);
     * ```
     *
     * @param string $clusterId    (`OutputOnly`)
     *                             The unique id of the cluster. Ex. us-central1-c
     * @param string $locationId   (`CreationOnly`)
     *                             The location where this cluster's nodes and storage reside. For best
     *                             performance, clients should be located as close as possible to this
     *                             cluster. Currently only zones are supported. Ex. cluster1
     * @param int    $serveNodes   The number of nodes allocated to this cluster. More nodes enable higher
     *                             throughput and more consistent performance.
     * @param array  $optionalArgs {
     *                             Optional.
     *
     *     @type int $state
     *          (`OutputOnly`)
     *          The current state of the cluster.
     *          For allowed values, use constants defined on {@see \Google\Cloud\Bigtable\Admin\V2\Cluster_State}
     *     @type int $defaultStorageType
     *          (`CreationOnly`)
     *          The type of storage used by this cluster to serve its
     *          parent instance's tables, unless explicitly overridden.
     *          For allowed values, use constants defined on {@see \Google\Cloud\Bigtable\Admin\V2\StorageType}
     *     @type RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\ApiCore\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\ApiCore\RetrySettings} for example usage.
     * }
     *
     * @return \Google\ApiCore\OperationResponse
     *
     * @throws ApiException if the remote call fails
     */
    public function updateCluster($clusterId, $locationId, $serveNodes, $optionalArgs = [])
    {
        $formattedName = $this->clusterName($clusterId);
        $location = $this->locationName($locationId);
        $operationResponse = $this->instanceAdminClient->updateCluster($formattedName, $location, $serveNodes, $optionalArgs);
        return $operationResponse;
    }

    /**
     * Deletes a cluster from an instance.
     *
     * Example:
     * ```
     * $clusterId = "cluster";
     * $instance->deleteCluster($clusterId);
     * ```
     *
     * @param string $clusterId         The unique id of the cluster to be deleted.
     *
     * @param array  $optionalArgs {
     *                             Optional.
     *
     *     @type RetrySettings|array $retrySettings
     *          Retry settings to use for this call. Can be a
     *          {@see Google\ApiCore\RetrySettings} object, or an associative array
     *          of retry settings parameters. See the documentation on
     *          {@see Google\ApiCore\RetrySettings} for example usage.
     * }
     *
     * @throws ApiException if the remote call fails
     */
    public function deleteCluster($clusterId, $optionalArgs = [])
    {
        $formattedName = $this->clusterName($clusterId);
        $this->instanceAdminClient->deleteCluster($formattedName);
    }
}
