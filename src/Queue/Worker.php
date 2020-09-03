<?php

namespace Nuwber\Events\Queue;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Cache\Repository;
use Interop\Amqp\AmqpConsumer;
use Nuwber\Events\Queue\ProcessingOptions;
use Nuwber\Events\Queue\MessageProcessor;
use PhpAmqpLib\Exception\AMQPRuntimeException;

class Worker
{
    /**
     * Indicates if the listener should exit.
     *
     * @var bool
     */
    public $shouldQuit;

    /** @var AmqpConsumer */
    private $consumer;

    /**
     * @var \Nuwber\Events\Queue\MessageProcessor
     */
    private $processor;

    /** @var ExceptionHandler */
    private $exceptions;
    
    /**
     * The cache repository implementation.
     *
     * @var \Illuminate\Contracts\Cache\Repository
     */
    protected $cache;

    public function __construct(AmqpConsumer $consumer, MessageProcessor $processor, ExceptionHandler $exceptions, Repository $cache)
    {
        $this->consumer = $consumer;
        $this->processor = $processor;
        $this->exceptions = $exceptions;
        $this->cache = $cache;
    }

    public function work(ProcessingOptions $options)
    {
        $this->listenForSignals();

        $lastRestart = $this->getTimestampOfLastQueueRestart();
        
        while (true) {
            if ($message = $this->getNextMessage($options->timeout)) {
                $this->processor->process($message);

                $this->consumer->acknowledge($message);
            }
            $this->stopIfNecessary($options, $lastRestart);
        }
    }

    /**
     * Receive next message from queuer
     *
     * @param int $timeout
     * @return \Interop\Amqp\AmqpMessage|null
     */
    protected function getNextMessage(int $timeout = 0)
    {
        try {
            return $this->consumer->receive($timeout);
        } catch (\Exception $e) {
            $this->exceptions->report($e);

            $this->stopListeningIfLostConnection($e);
        }
    }

    protected function stopListeningIfLostConnection($exception)
    {
        if ($exception instanceof AMQPRuntimeException) {
            $this->shouldQuit = true;
        }
    }

    /**
     * Stop the process if necessary.
     *
     * @param  ProcessingOptions $options
     */
    protected function stopIfNecessary(ProcessingOptions $options, $lastRestart)
    {
        if ($this->shouldQuit) {
            $this->stop();
        } else if ($this->memoryExceeded($options->memory)) {
            $this->stop(12);
        } elseif ($this->queueShouldRestart($lastRestart)) {
            $this->stop();
        }
    }

    /**
     * Determine if the memory limit has been exceeded.
     *
     * @param  int $memoryLimit
     * @return bool
     */
    protected function memoryExceeded($memoryLimit)
    {
        return (memory_get_usage(true) / 1024 / 1024) >= $memoryLimit;
    }

    /**
     * Stop listening and bail out of the script.
     *
     * @param  int $status
     * @return void
     */
    protected function stop($status = 0)
    {
        exit($status);
    }

    /**
     * Enable async signals for the process.
     *
     * @return void
     */
    protected function listenForSignals()
    {
        pcntl_async_signals(true);

        foreach ([SIGINT, SIGTERM, SIGALRM] as $signal) {
            pcntl_signal($signal, function () {
                $this->shouldQuit = true;
            });
        }
    }
    
    /**
     * Determine if the queue worker should restart.
     *
     * @param  int|null  $lastRestart
     * @return bool
     */
    protected function queueShouldRestart($lastRestart)
    {
        return $this->getTimestampOfLastQueueRestart() != $lastRestart;
    }

    /**
     * Get the last queue restart timestamp, or null.
     *
     * @return int|null
     */
    protected function getTimestampOfLastQueueRestart()
    {
        if ($this->cache) {
            return $this->cache->get('illuminate:queue:restart');
        }
    }
}
