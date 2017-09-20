# MongoDBLib
mongoDB 3 for Codeigniter

# HOW TO USE

## load config in CI

```
$config['mongo'] = [
		'hostname' => '192.168.0.1',
		'port' => 27017,
		'username' => '',
		'password' => '',
		'database' => 'mydatabase',
		'dbdriver' => 'mongodb',
		'db_debug' => (ENVIRONMENT !== 'production')
	];
```

## load library in your controller with config

```
$mc = $this->config->item('mongo');
$this->load->library('mongo', $mc);
$this->db = $this->mongo;
```

## done!

enjoy!
