<?php

namespace Shishima\KafkaQueue;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;

use RdKafka\KafkaConsumer;
use RdKafka\Producer;

use function Opis\Closure\unserialize;

class KafkaQueue extends Queue implements QueueContract
{
    protected Producer $producer;
    protected KafkaConsumer $consumer;

    public function __construct(KafkaConsumer $consumer,Producer $producer)
    {
        $this->consumer = $consumer;
        $this->producer = $producer;
    }

    public function size($queue = null)
    {
        // TODO: Implement size() method.
    }

    public function push($job, $data = '', $queue = null)
    {
        $topic = $this->producer->newTopic($queue);
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $this->producer->flush(1000);
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        // TODO: Implement pushRaw() method.
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        $topic = $this->producer->newTopic($queue);
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $this->producer->flush(5000);
    }

    public function pop($queue = null)
    {
        $this->consumer->subscribe([$queue]);
        try {
            $message = $this->consumer->consume(130 * 1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $job = unserialize($message->payload);
                    $job->handle();
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    var_dump("No more messages, will wait for more\n");
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    var_dump('Timeout');
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
            }
        } catch (\Exception $e) {
            var_dump($e->getMessage());
        }
    }
}
