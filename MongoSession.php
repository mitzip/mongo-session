<?php namespace Cballou;
/*
 * This MongoDB session handler is intended to store any data you see fit.
 * One interesting optimization to note is the setting of the active flag
 * to 0 when a session has expired. The intended purpose of this garbage
 * collection is to allow you to create a batch process for removal of
 * all expired sessions. This should most likely be implemented as a cronjob
 * script.
 *
 * @author	Corey Ballou
 * @copyright	Corey Ballou (2010)
 *
 * @author	DavidMitchel.com
 * @license	MIT
 *
 */
class MongoSession {

	// default config with support for multiple servers
	// (helpful for sharding and replication setups)
	protected $_config = array(
		// cookie related vars
		'cookie_path'   => '/', // cookie works on all paths
		'cookie_domain' => '.example.com', // subdomain wildcard for example.com

		// session related vars
		'name'		=> 'mongo_sess',// session name
		'lifetime'      => 3600,        // session lifetime in seconds
		'database'      => 'session',   // name of MongoDB database
		'collection'    => 'sessions',   // name of MongoDB collection

		// replicaSet name
		'replicaSet'		=> '',

		// array of mongo db servers
		'servers'   	=> array(
			array(
			'host'          => \Mongo::DEFAULT_HOST,
			'port'          => \Mongo::DEFAULT_PORT,
                	'username'      => null,
                	'password'      => null
            		)
		)
	);

	// stores the connection
	protected $_connection;

	// stores the mongo db
	protected $_mongo;

	// session id
	protected $_session_id;

	// stores session data results
	protected $_session;

	/**
	 * Default constructor.
	 *
	 * @access	public
	 * @param	array	$config
	 */
	public function __construct($config = array()) {
		// writes session before __destruct is called
		register_shutdown_function('session_write_close');

		// initialize the database
		$this->_init(empty($config) ? $this->_config : $config);

		// set object as the save handler
		session_set_save_handler(
			array(&$this, 'open'),
			array(&$this, 'close'),
			array(&$this, 'read'),
			array(&$this, 'write'),
			array(&$this, 'destroy'),
			array(&$this, 'gc')
		);

		// set some important session vars
		ini_set('session.auto_start',               0);
		ini_set('session.gc_probability',           1);
		ini_set('session.gc_divisor',               100);
		ini_set('session.gc_maxlifetime',           $this->_config['lifetime']);
		ini_set('session.use_cookies',              1);
		ini_set('session.use_only_cookies',         1);
		ini_set('session.use_trans_sid',            0);
		ini_set('session.name',                     $this->_config['name']);
		ini_set('session.cookie_lifetime',          $this->_config['lifetime']);
		ini_set('session.cookie_path',              $this->_config['cookie_path']);
		ini_set('session.cookie_domain',            $this->_config['cookie_domain']);

		// disable client/proxy caching
		session_cache_limiter('nocache');

		// set session id
		if (isset($_COOKIE[$this->_config['name']])) {
			$this->read($_COOKIE[$this->_config['name']],1);
		}

		if (!isset($this->_session_id)) {
			session_id((string) new \MongoId());
		}

		// start it up
		session_start();
	}

	/**
	 * Initialize MongoDB.
	 *
	 * @access	private
	 * @param	array	$config
	 */
	private function _init($config) {
		// ensure they supplied a database
		if (empty($config['database'])) {
			throw new Exception('You must specify a MongoDB database to use for session storage.');
		}

		if (empty($config['collection'])) {
			throw new Exception('You must specify a MongoDB collection to use for session storage.');
		}

		// update config
		$this->_config = $config;

		// generate server connection strings
		$connections = array();
		if (!empty($this->_config['servers'])) {
			foreach ($this->_config['servers'] as $server) {
				$str = '';
				if (!empty($server['username']) && !empty($server['password'])) {
					$str .= $server['username'] . ':' . $server['password'] . '@';
				}
				$str .= !empty($server['host']) ? $server['host'] : \Mongo::DEFAULT_HOST;
				$str .= ':' . (!empty($server['port']) ? (int) $server['port'] : \Mongo::DEFAULT_PORT);
				array_push($connections, $str);
			}
		} else {
			// use default connection settings
			array_push($connections, \Mongo::DEFAULT_HOST . ':' . \Mongo::DEFAULT_PORT);
		}

		// add immediate connection, although this is the default driver action
		$opts = array('connect' => true);

		// support replica sets
		if ($this->_config['replicaSet'] != '') {
			$opts['replicaSet'] = $this->_config['replicaSet'];
		}

		// load mongo server connection
		try {
			$this->_connection = new \Mongo('mongodb://' . implode(',', $connections), $opts);
		} catch (Exception $e) {
			throw new Exception('Can\'t connect to the MongoDB server.');
		}

		// load the db
		try {
			$mongo = $this->_connection->selectDB($this->_config['database']);
		} catch (InvalidArgumentException $e) {
			throw new Exception('The MongoDB database specified in the config does not exist.');
		}

		// load collection
		try {
			$this->_mongo = $mongo->selectCollection($this->_config['collection']);
		} catch(Exception $e) {
			throw new Exception('The MongoDB collection specified in the config does not exist.');
		}

		// proper indexing on the expiration
		$this->_mongo->ensureIndex(
			array('expiry' => 1),
			array('name' => 'expiry',
				'unique' => 1,
				'dropDups' => 1,
				'safe' => 1,
				'sparse' => 1
			)
		);

		// proper indexing of session id and lock
		$this->_mongo->ensureIndex(
			array('_id' => 1, 'lock' => 1),
			array('name' => '_id_lock',
				'unique' => 1,
				'dropDups' => 1,
				'safe' => 1
			)
		);
	}

	/**
	 * Open does absolutely nothing as we already have an open connection.
	 *
	 * @access	public
	 * @return	bool
	 */
	public function open($save_path, $session_name) {
		return true;
	}

	/**
	 * Close does absolutely nothing as we can assume __destruct handles
	 * things just fine.
	 *
	 * @access	public
	 * @return	bool
	 */
	public function close() {
		return true;
	}

	/**
	 * Read the session data.
	 *
	 * @access	public
	 * @param	string	$id
	 * @return	string
	 */
	public function read($id, $internal = null) {
		// obtain a read lock on the data, or subsequently wait for
		// the lock to be released
		!$internal ? $this->_lock($id) : '';

                // Convert $id to proper MongoID
		$id = new \MongoId($id);

		// exclude results that are inactive or expired
		$result = $this->_mongo->findOne(
			array(
				'_id'		=> $id,
				'expiry'    	=> array('$gte' => time()),
				'active'    	=> 1
			)
		);

		if (!empty($result)) {
			$this->_session_id = $result['_id'];
			unset($result['_id']);
			$this->_session = $result;
			if (isset($result['data'])) {
				return $result['data'];
			}
		}

		return '';
	}

	/**
	 * Atomically write data to the session, ensuring we remove any
	 * read locks.
	 *
	 * @access	public
	 * @param	string	$id
	 * @param	mixed	$data
	 * @return	bool
	 */
	public function write($id, $data) {
		// Convert $id to proper MongoID
		$id = new \MongoId($id);

		// create expires
		$expiry = time() + $this->_config['lifetime'];

		// create new session data
		$new_obj = array(
			'data'		=> $data,
			'lock'		=> 0,
			'active'	=> 1,
			'expiry'	=> $expiry
		);

		// check for existing session for merge
		if (isset($this->_session)) {
			$obj = (array) $this->_session;
			$new_obj = array_merge($obj, $new_obj);
		}

		// atomic update
		$query = array('_id' => $id);

		// update options
		$options = array(
			'safe'		=> 1,
			'fsync'		=> 1
		);

		// perform the update
		try {
			$result = $this->_mongo->update($query, array('$set' => $new_obj), $options);
			return $result['ok'] == 1;
		} catch (Exception $e) {
			return false;
		}

		return true;
	}

	/**
	 * Destroys the session by removing the document with
	 * matching session_id.
	 *
	 * @access	public
	 * @param	string	$id
	 * @return	bool
	 */
	public function destroy($id) {
		// Convert $id to proper MongoID
		$id = new \MongoId($id);

		$this->_mongo->remove(array('_id' => $id), true);
		return true;
	}

	/**
	 * Garbage collection. Remove all expired entries atomically.
	 *
	 * @access	public
	 * @return	bool
	 */
	public function gc() {
		// define the query
		$query = array('expiry' => array('$lt' => time()));

		// specify the update vars
		$update = array('$set' => array('active' => 0));

		// update options
		$options = array(
			'multiple'	=> 1,
			'safe'		=> 1,
			'fsync'		=> 1
		);

		// update expired elements and set to inactive
		$this->_mongo->update($query, $update, $options);
		return true;
	}

	/**
	 * Create a global lock for the specified document.
	 *
	 * @author	Benson Wong (mostlygeek@gmail.com)
	 * @access	private
	 * @param	string	$id
	 */
	private function _lock($id) {
		// Convert $id to proper MongoID
		$id = new \MongoId($id);

		$remaining = 30000000;
		$timeout = 5000;

		$openSession = array('_id' => $id, 'lock' => 0);
		$lockedSession = array('_id' => $id, 'lock' => 1);
		$lock = array('$set' => array('lock' => 1));
		$options = array('safe' => true);

		do {
			try {
				if ($this->_mongo->findOne($openSession)) {
					$result = $this->_mongo->update($openSession, $lock, $options);
				} else {
					$result = $this->_mongo->insert($lockedSession, $options);
				}
				if ($result['ok'] == 1) {
					return true;
				}
			} catch (MongoCursorException $e) {
				if (substr($e->getMessage(), 0, 26) != 'E11000 duplicate key error') {
				throw $e; // not a dup key?
			}
		}

		// force delay in microseconds
		usleep($timeout);
		$remaining -= $timeout;

		// backoff on timeout, save a tree. max wait 1 second
		$timeout = ($timeout < 1000000) ? $timeout * 2 : 1000000;

		} while ($remaining > 0);
			// aww shit.
			throw new Exception('Could not obtain a session lock.');
		}
}
