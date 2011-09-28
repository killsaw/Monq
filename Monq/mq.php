<?php

namespace Monq;

use AmqpExchange, AmqpQueue, AmqpConnection;

class mq
{
	protected static $conn = array();
	public static $debug = true;
	
	public static function connect($config, $name='default')
	{
		if (!isset(self::$conn[$name]) || !self::$conn[$name]->isConnected()) {
			
			if (is_string($config)) {
				$config = parse_url($config);
				list($vhost, $exchange) = explode('/', $config['path']);
				if (empty($vhost)) {
					$vhost = '/';
				}
				$config['vhost'] = $vhost;
				$config['exchange'] = $exchange;
				unset($config['path']);
			}
			
			self::debug('Establishing queue server connection (%s).', $name);
			
			$connection = new MqConnection;
			$connection->setHost($config['host']);
			$connection->setLogin($config['user']);
			$connection->setPassword($config['pass']);
			$connection->setVhost($config['vhost']);
			$connection->setExchange($config['exchange']);
			$connection->connect();

			self::debug('Connection established.');
			self::$conn[$name] = $connection;
		}
		
		return self::$conn[$name];
	}
	
	public static function getConnection($name='default')
	{
		if (isset(self::$conn[$name]) && self::$conn[$name] instanceof MqConnection) {
			return self::$conn[$name];
		} else {
			return false;
		}
	}
	
	public static function reconnect()
	{
		return self::getConnection()->connect();
	}
		
	public static function disconnect()
	{
		return self::getConnection()->disconnect();
	}
	
	public static function getExchange()
	{
		return self::getConnection()->getExchange();
	}

	public static function getQueue($queue_name, $routing_key=null, $durable=true)
	{
		if (is_null($routing_key)) {
			$routing_key = $queue_name;
		}
		return self::getConnection()->getQueue($queue_name, $routing_key, $durable);
	}
	
	public static function resolveQueueName($queue_name)
	{
		return self::getConnection()->getKeyPrefix().$queue_name;
	}
	
	public static function publish($key, $message, $opts=array())
	{
		self::debug("Sending message to key '%s' on default connection.", $key);
		
		$params = array_merge(array(
			'Content-type'=>'application/json',
			'Content-encoding'=>NULL,
			'message_id'=>NULL,
			'correlation_id'=>NULL,
			'user_id'=>NULL,
			'app_id'=>NULL,
			'delivery_mode'=>2,
			'priority'=>NULL,
			'timestamp'=>time(),
			'expiration'=>NULL,
			'type'=>NULL,
			'reply_to'=>NULL
		), $opts);
		
		if (!is_string($message)) {
			// May need to utf8encode() binary junk
			$message = json_encode($message);
		}
		
		return self::getExchange()->publish(
				$message, 
				$key,
				null,
				$params
			);	
	}
	
	public static function consume(AMQPQueue $queue, $callback, $options=array())
	{
		if (!is_callable($callback)) {
			throw new MqException("Callback is not callable.");
		}
		
		$options = array_merge($options, array(
			'max_cycles' => -1,
			'min'=>1,
			'max'=>1,
			'ack'=>false
		));
		
		$i = 1;
		self::debug('Beginning long-polling queue read.');
		
		while($i++) {
			if ($options['max_cycles'] > 0 && 
				$i > $options['max_cycles']) {
				break;
			}
			
			try {
				$messages = $queue->consume(array(
					'min'=>$options['min'],
					'max'=>$options['max'],
					'ack'=>$options['ack']
				));
				self::debug('Message(s) received from queue.');
				
				foreach($messages as $msg) {
					if ($msg['Content-type'] == 'application/json') {
						$msg['message_body'] = json_decode($msg['message_body']);
					}
					
					if (call_user_func($callback, $msg)) {
						self::debug("Ack'ing queue message.");
						$queue->ack($msg['delivery_tag']);
					}
				}
			} catch (\Exception $e) {
				self::debug('Queue exception: %s', $e->getMessage());
				print_r($e);
				self::reconnect();
				// Do I need to refresh the queue object?
				sleep(10);
			}
		}
	}
	
	public static function rpc()
	{
		$rpc = new mqRPC;
		$rpc->setConnection(self::getConnection());
		return $rpc;
	}
	
	public static function debug($msg)
	{
		$args = func_get_args();
		$msg = array_shift($args);
		if (self::$debug) {
			printf("[%s] %s\n", 
				date('M/d/Y G:i:s'), 
				vsprintf($msg, $args)
			);
		}
	}
}