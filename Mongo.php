<?php
	defined('BASEPATH') OR exit('No direct script access allowed');

	/*
	 *author: Sober
	 *
	*/
	class Mongo{

		private $CI;
		private $manager;
		private $connection_string;
		private $collection;
		private $namespace;
		
		private $host;
		private $port;
		private $user;
		private $pass;
		private $dbname;
		private $persist;
		private $persist_key;
		
		private $selects = ['_id' => 0];
		private $wheres = [];
		private $sorts = [];
		
		private $limit = 999999;
		private $offset = 0;
		
		
		//---- server
		public function __construct($param){
			$this->CI =& get_instance();
			$this->connection_string($param);
			$this->connect();
		}

		private function connection_string(array $param) {
			//mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
			@$this->host = trim($param['hostname']);
			@$this->port = trim($param['port']);
			@$this->user = trim($param['user']);
			@$this->pass = trim($param['pass']);
			@$this->dbname = trim($param['database']);
			@$this->persist = trim($param['persist']);
			@$this->persist_key = trim($param['persist_key']);
			
			$connection_string = "mongodb://";
			
			if(empty($this->host)) show_error("The Host must be set to connect to MongoDB", 500);

			if(empty($this->dbname)) show_error("The Database must be set to connect to MongoDB", 500);
			
			if(!empty($this->user) && !empty($this->pass)) $connection_string .= "{$this->user}:{$this->pass}@";
			
			$connection_string .= "{$this->host}";

			if(isset($this->port) && !empty($this->port)) $connection_string .= ":{$this->port}";
			
			$this->connection_string = trim($connection_string);
		}

		private function connect() {
			$uriOptions = array();
			if($this->persist === TRUE)
				$uriOptions['replicaSet'] = isset($this->persist_key) && !empty($this->persist_key) ? $this->persist_key : 'ci_mongo_persist';

			$this->manager = new MongoDB\Driver\Manager($this->connection_string, $uriOptions);
			//$this->bulk = new MongoDB\Driver\BulkWrite;
			$cmd = new MongoDB\Driver\Command(["ping" => 1]);
			$returns = [];

			try{
				$cursor = $this->manager->executeCommand($this->dbname, $cmd);
				$returns = $cursor->toArray();
			} catch (MongoDB\Driver\Exception\Exception $e) {
				$filename = basename(__FILE__);
				$str = '';
				$str .= " The $filename script has experienced an error.";
				$str .= " It failed with the following exception:";
				$str .= " Exception:" . $e->getMessage();
				$str .= " In file:" . $e->getFile();
				$str .= " On line:" . $e->getLine();
				show_error($str);
			}
			(current($returns)->ok == 1) or show_error('db connect failed.');
			return $this;	
		}

		public function switch_db($database = '') {
			!empty($database) or show_error("To switch MongoDB databases, a new database name must be specified", 500);
			$this->dbname = trim($database);
			if(!empty($this->collection)) $this->namespace = implode('.', [$this->dbname, $this->collection]);
		}

		public function from($collection = '') {
			!empty($collection) or show_error("In order to retreive documents from MongoDB, a collection name must be passed", 500);
			$this->collection = trim($collection);
			$this->namespace = implode('.', [$this->dbname, $this->collection]);
		}

		public function select(array $includes = [], array $excludes = []) {
			if(!empty($includes)) foreach($includes as $col) $this->selects[$col] = 1;
			if(!empty($excludes)) foreach($excludes as $col) $this->selects[$col] = 0;
			return $this;
		}

		public function where(array $wheres = []) {
			foreach($wheres as $wh => $val) $this->wheres[$wh] = $val;
			return $this;
		}

		public function where_in($field = "", array $in = []) {
			$this->where_init($field);
			$this->wheres[$field]['$in'] = $in;
			return $this;
		}

		public function where_not_in($field = "", array $in = []) {
			$this->where_init($field);
			$this->wheres[$field]['$nin'] = $in;
			return $this;
		}

		public function where_gt($field = "", $x) {
			$this->where_init($field);
			$this->wheres[$field]['$gt'] = $x;
			return $this;
		}

		public function where_gte($field = "", $x) {
			$this->where_init($field);
			$this->wheres[$field]['$gte'] = $x;
			return $this;
		}

		public function where_lt($field = "", $x) {
			$this->where_init($field);
			$this->wheres[$field]['$lt'] = $x;
			return $this;
		}

		public function where_lte($field = "", $x) {
			$this->where_init($field);
			$this->wheres[$field]['$lte'] = $x;
			return $this;
		}

		public function where_between($field = "", $x, $y) {
			$this->where_init($field);
			$this->wheres[$field]['$gte'] = $x;
			$this->wheres[$field]['$lte'] = $y;
			return $this;
		}

		public function where_between_ne($field = "", $x, $y) {
			$this->where_init($field);
			$this->wheres[$field]['$gt'] = $x;
			$this->wheres[$field]['$lt'] = $y;
			return $this;
		}

		public function where_ne($field = "", $x) {
			$this->where_init($field);
			$this->wheres[$field]['$ne'] = $x;
			return $this;
		}

		public function like($field = "", $value = "", $flags = "i", $enable_start_wildcard = TRUE, $enable_end_wildcard = TRUE) {
			$field = (string) trim($field);
			$this->where_init($field);
			$value = (string) trim($value);
			$value = quotemeta($value);
			if($enable_start_wildcard !== TRUE) $value = "^" . $value;
			if($enable_end_wildcard !== TRUE) $value .= "$";
			$regex = "/$value/$flags";
			$this->wheres[$field]['$regex'] = $regex;
			return $this;
		}

		public function order_by(array $fields = []) {
			foreach($fields as $col => $val) $this->sorts[$col] = ($val == -1 || $val === FALSE || strtolower($val) == 'desc')? -1 : 1;
			return $this;
		}

		public function limit(int $x = 99999) {
			if($x >= 1) $this->limit = $x;
			return $this;
		}

		public function offset(int $x = 0) {
			if($x >= 1) $this->offset = $x;
			return $this;
		}

		public function get_where($collection = "", array $where = [], $limit = 99999) {
			return($this->where($where)->limit($limit)->get($collection));
		}

		public function get($collection = "") {
			$this->from($collection);
			$options = ['projection' => $this->selects, 'sort' => $this->sorts, 'limit' => $this->limit, 'skip' => $this->offset];
			$returns = [];
			try {
				$query = new MongoDB\Driver\Query($this->wheres, $options);
				$cursor = $this->manager->executeQuery($this->namespace, $query);
				$returns = $cursor->toArray();
			} catch (MongoDB\Driver\Exception\BulkWriteException $e) {
				$this->BWException($e);
			} catch (MongoDB\Driver\Exception\Exception $e) {
				show_error("Other error: " . $e->getMessage());
			}
			$this->clear();
			return $returns;
		}

		public function count($collection = "", $option = []) {
			$this->from($collection);
			$cmd = new MongoDB\Driver\Command([
				'count' => $this->collection,
				'query' => $option
			]);
			$returns = [];
			try{
				$cursor = $this->manager->executeCommand($this->dbname, $cmd);
				$returns = current($cursor->toArray());
			} catch (MongoDB\Driver\Exception\Exception $e) {
				$filename = basename(__FILE__);
				$str = '';
				$str .= " The $filename script has experienced an error.";
				$str .= " It failed with the following exception:";
				$str .= " Exception:" . $e->getMessage();
				$str .= " In file:" . $e->getFile();
				$str .= " On line:" . $e->getLine();
				show_error($str);
			}

			$this->clear();
			return $returns->n;
		}

		public function insert($collection = "", array $data = []) {
			$this->from($collection);
			(count($data) != 0) or show_error("Nothing to insert into Mongo collection or insert is not an array", 500);
			
			try {
				$bulk = new MongoDB\Driver\BulkWrite;
				$bulk->insert($data);
				$writeResult = $this->manager->executeBulkWrite($this->namespace, $bulk);
			} catch(MongoDB\Driver\Exception\BulkWriteException $e) {
				BWException("Insert of data into MongoDB failed", $e);
			} catch (MongoDB\Driver\Exception\Exception $e) {
				show_error("Other error: " . $e->getMessage());
			}
			$this->clear();
			return $writeResult->getInsertedCount() == 1;
		}

		public function update($collection = "", array $data = [], bool $insertWhenEmpty = false, bool $patchAll = false) {
			$this->from($collection);
			(count($data) != 0) or show_error("Nothing to update in Mongo collection or update is not an array", 500);

			$option = ['upsert' => $insertWhenEmpty, 'multi' => $patchAll];
			if($insertWhenEmpty) $option['multi'] = false;
			try {
				$bulk = new MongoDB\Driver\BulkWrite;
				$bulk->update($this->wheres, $data, $option);
				$writeResult = $this->manager->executeBulkWrite($this->namespace, $bulk);
			} catch(MongoDB\Driver\Exception\BulkWriteException $e) {
				BWException("Update of data into MongoDB failed", $e);
			} catch (MongoDB\Driver\Exception\Exception $e) {
				show_error("Other error: " . $e->getMessage());
			}
			$this->clear();
			return ($insertWhenEmpty? $writeResult->getUpsertedCount() : $writeResult->getModifiedCount()) == 1;
		}

		public function update_all($collection = "", array $data = []){
			return $this->update($collection = "", $data = [], false, true);
		}

		public function delete($collection = "", int $justOne = 1) {
			$this->from($collection);
			
			$option = ['limit' => $justOne];
			try {
				$bulk = new MongoDB\Driver\BulkWrite;
				$bulk->delete($this->wheres, $option);
				$writeResult = $this->manager->executeBulkWrite($this->namespace, $bulk);
			} catch(MongoDB\Driver\Exception\BulkWriteException $e) {
				BWException("Delete of data into MongoDB failed", $e);
			} catch (MongoDB\Driver\Exception\Exception $e) {
				show_error("Other error: " . $e->getMessage());
			}
			$this->clear();
			return $writeResult->getDeletedCount();
		}

		public function delete_all($collection = ""){
			return $this->delete($collection, false);
		}

		public function empty_table($collection = ""){
			$this->wheres = [];
			return $this->delete_all($collection);
		}

		public function truncate($collection = ""){
			return $this->empty_table();
		}

		public function insert_batch($collection = "", array $data = []){
			$this->from($collection);
			(count($data) != 0) or show_error("Nothing to insert into Mongo collection or insert is not an array", 500);
			
			try {
				$bulk = new MongoDB\Driver\BulkWrite;
				foreach ($data as $doc) {
					$bulk->insert($doc);
				}
				$writeResult = $this->manager->executeBulkWrite($this->namespace, $bulk);
			} catch(MongoDB\Driver\Exception\BulkWriteException $e) {
				BWException("Insert tons of data into MongoDB failed", $e);
			} catch (MongoDB\Driver\Exception\Exception $e) {
				show_error("Other error: " . $e->getMessage());
			}

			return $writeResult->getInsertedCount();
			
		}
		/*
		public function update_batch($collection = "", array $data = []){
			$this->from($collection);
			(count($data) != 0) or show_error("Nothing to update Mongo collection or update is not an array", 500);

			try {
				$bulk = new MongoDB\Driver\BulkWrite;
				foreach ($data as $doc) {
					$bulk->update($doc);
				}
				$writeResult = $this->manager->executeBulkWrite($this->namespace, $bulk);
			} catch(MongoDB\Driver\Exception\BulkWriteException $e) {
				BWException("Update tons of data into MongoDB failed", $e);
			} catch (MongoDB\Driver\Exception\Exception $e) {
				show_error("Other error: " . $e->getMessage());
			}
		}
		*/
		public function reset_query(){
			$this->clear();
		}

		private function clear() {
			$this->selects = [];
			$this->wheres = [];
			$this->limit = NULL;
			$this->offset = NULL;
			$this->sorts = [];
		}

		private function where_init($param) {
			if(!isset($this->wheres[$param])) $this->wheres[$param] = [];
		}

		private function BWException($e){
			$result = $e->getWriteResult();
			// Check if the write concern could not be fulfilled
			if ($writeConcernError = $result->getWriteConcernError()) {
				show_error(
					$writeConcernError->getMessage(),
					$writeConcernError->getCode(),
					var_export($writeConcernError->getInfo(), true)
				);
			}
			// Check if any write operations did not complete at all
			$str = '';
			foreach ($result->getWriteErrors() as $writeError) {
				$str .= implode(' ', ["Operation#",
					$writeError->getIndex(),
					$writeError->getMessage(),
					$writeError->getCode()
				]);
			}
			show_error($str);
		}
	}
