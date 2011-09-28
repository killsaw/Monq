<?php

namespace Monq;

class mqRPC
{
	protected $connection;
	
	public function setConnection(mqConnection $conn)
	{
		$this->connection = $conn;
		return $this;
	}
	
	public function call($call_name, array $args=array(), $timeout=false)
	{
		// The queue we receive a reply on.
		$callback_queue_name = uniqid('mqrpc_callback');
		$callback_queue = mq::getQueue($callback_queue_name, $callback_queue_name, $durable=false);
		
		$correlation_id = uniqid('mqrpc.'.microtime(true));
		
		$args['cmd'] = $call_name;
		$args['correlation_id'] = $correlation_id;
		
		$params = array(
			'correlation_id'=>$correlation_id,
			'reply_to'=>$callback_queue_name,
			'timestamp'=>NULL,
			'expiration'=>NULL
		);
		
		if ($timeout) {
			$params['expiration'] = $timeout * 1000;
		}
		
		$start_ts = time();
		mq::publish(sprintf('rpc.%s', $call_name), $args, $params);
		
		do {
			mq::debug('Polling for RPC reply.');
			$message = $callback_queue->get(AMQP_NOACK);
			
			if ($timeout && ((time() - $start_ts) > $timeout)) {
				mq::debug("RPC read loop exceeded required timeout (%.2fs)", $timeout);
				return false;
			}
			
			if (isset($message['count']) && $message['count'] < 0) {
				usleep(500000);
				continue;
			}
			
			$payload = json_decode($message['msg']);
			
			if ($payload->correlation_id != $correlation_id) {
				mq::debug("Got message, but it wasn't the right one.");
				usleep(500000); // Wait for half a second.
				continue;
			}
			$callback_queue->ack($message['delivery_tag']);
			//$callback_queue->delete(mq::resolveQueueName($callback_queue_name));
			break;
		
		} while (true);
		
		return $payload;
	}
	
	public function serve($routing_key, $callback=null)
	{
		$routing_key = sprintf('rpc.%s', $routing_key);
		
		$rpc_queue = mq::getQueue($routing_key, $routing_key);
		$params = array(
			'message_id'=>NULL,
			'correlation_id'=>NULL,
			'reply_to'=>''
		);
		
		do {
			mq::debug("Waiting for messages.");
			
			$messages = $rpc_queue->consume(array(
				'min'=>1,
				'max'=>10,
				'ack'=>false
			));
			
			mq::debug("Received %d messages from queue.", count($messages));
			
			foreach($messages as $k=>$message) {
				mq::debug('Processing RPC message %d.', $k);
				$payload = json_decode($message['message_body']);
				
				// Do the action
				$args = (array)$payload;
				if (isset($args['correlation_id'])) {
					unset($args['correlation_id']);
				}
				if (isset($args['cmd'])) {
					unset($args['cmd']);
				}
				
				$cmd_name = str_replace('.', '::', $payload->cmd);
				
				if (is_callable($callback)) {
					$result = call_user_func_array($callback, $args);
				} elseif(is_callable($cmd_name)) {
					$result = call_user_func_array($cmd_name, $args);
				} else {
					$reslt = "$cmd_name isn't callable.";
				}
				$reply = array();
				$reply['result'] = $result;
				$reply['correlation_id'] = $payload->correlation_id;
				
				mq::debug("Publishing reply back to routing key '%s'", $message['Reply-to']);
				$ok = mq::publish($message['Reply-to'], $reply);

				mq::debug('Acking RPC message.');
				$rpc_queue->ack($message['delivery_tag']);
			}
						
		} while(true);
	}
}