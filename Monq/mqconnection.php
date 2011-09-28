<?php

namespace Monq;

use AmqpExchange, AmqpQueue, AmqpConnection;

class mqConnection extends AmqpConnection
{
	protected $keyPrefix = '';
	protected $exchange = '';
	protected $exchangeName = '';
	protected $queues = array();
	
	public function setExchange($exchange_name)
	{
		$this->exchangeName = $exchange_name;
		return $this;
	}
	
	public function getExchange()
	{
		if (!isset($this->exchange) || !($this->exchange instanceof AmqpExchange)) {
			$exchange = new AMQPExchange($this);
			$exchange->declare($this->exchangeName, AMQP_EX_TYPE_TOPIC, AMQP_DURABLE);
			$this->exchange = $exchange;
		}		
		return $this->exchange;
	}
	
	public function getQueue($queue_name, $routing_key, $durable=true)
	{
		$exchange = $this->getExchange();
		$queue = new AMQPQueue($this);
		
		if (isset($this->keyPrefix)) {
			$queue_name = sprintf('%s%s', $this->keyPrefix, $queue_name);
		}
		
		if (is_null($queue_name) || !$durable) {
			mq::debug("Declaring null queue with auto-delete option.");
			$queue->declare($queue_name, AMQP_AUTODELETE);
		} else {
			mq::debug("Declaring durable queue '%s'", $queue_name);
			$queue->declare($queue_name, AMQP_DURABLE);
		}
		
		foreach((array)$routing_key as $rk) {
			mq::debug("Binding queue '%s' to routing key '%s'", $queue_name, $rk);
			$queue->bind($this->exchangeName, $rk);
		}
		return $queue;
	}
	
	public function getKeyPrefix()
	{
		return $this->keyPrefix;
	}
	
	public function setKeyPrefix($key_prefix)
	{
		$this->keyPrefix = $key_prefix;
		return $this;
	}
}