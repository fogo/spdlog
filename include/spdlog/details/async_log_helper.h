//
// Copyright(c) 2015 Gabi Melman.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)
//

// async log helper :
// Process logs asynchronously using a back thread.
//
// If the internal queue of log messages reaches its max size,
// then the client call will block until there is more room.
//

#pragma once

#include "spdlog/common.h"
#include "spdlog/sinks/sink.h"
#include "spdlog/details/mpmc_bounded_q.h"
#include "spdlog/details/log_msg.h"
#include "spdlog/details/os.h"
#include "spdlog/formatter.h"

#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <deque>
#include <unordered_map>

namespace spdlog
{
namespace details
{

class base_async_log_helper
{
protected:
    // Async msg to move to/from the queue
    // Movable only. should never be copied
    enum class async_msg_type
    {
        log,
        flush,
        terminate
    };
    struct async_msg
    {
        std::string logger_name;
        level::level_enum level;
        log_clock::time_point time;
        size_t thread_id;
        std::string txt;
        async_msg_type msg_type;
        size_t msg_id;

        async_msg() = default;
        ~async_msg() = default;


        async_msg(async_msg&& other) SPDLOG_NOEXCEPT:
            logger_name(std::move(other.logger_name)),
            level(std::move(other.level)),
            time(std::move(other.time)),
            thread_id(other.thread_id),
            txt(std::move(other.txt)),
            msg_type(std::move(other.msg_type)),
            msg_id(other.msg_id)
        {}

        async_msg(const std::string& l_name, async_msg_type m_type):
            logger_name(l_name),
            level(level::info),
            thread_id(0),
            msg_type(m_type),
            msg_id(0)
        {}

        async_msg& operator=(async_msg&& other) SPDLOG_NOEXCEPT
        {
            logger_name = std::move(other.logger_name);
            level = other.level;
            time = std::move(other.time);
            thread_id = other.thread_id;
            txt = std::move(other.txt);
            msg_type = other.msg_type;
            msg_id = other.msg_id;
            return *this;
        }

        // never copy or assign. should only be moved..
        async_msg(const async_msg&) = delete;
        async_msg& operator=(const async_msg& other) = delete;

        // construct from log_msg
        async_msg(const details::log_msg& m):
            level(m.level),
            time(m.time),
            thread_id(m.thread_id),
            txt(m.raw.data(), m.raw.size()),
            msg_type(async_msg_type::log),
            msg_id(m.msg_id)
        {
#ifndef SPDLOG_NO_NAME
            logger_name = *m.logger_name;
#endif
        }


        // copy into log_msg
        void fill_log_msg(log_msg &msg)
        {
            msg.logger_name = &logger_name;
            msg.level = level;
            msg.time = time;
            msg.thread_id = thread_id;
            msg.raw << txt;
            msg.msg_id = msg_id;
        }
    };

public:
    using item_type = async_msg;
    using q_type = details::mpmc_bounded_queue<item_type>;

    using clock = std::chrono::steady_clock;

    void log(const details::log_msg& msg);

    // stop logging and join the back thread
    virtual ~base_async_log_helper() {};

    virtual void set_formatter(const std::string& logger_name, formatter_ptr) = 0;
    virtual void flush(const std::string& logger_name, bool wait_for_q) = 0;
    virtual void set_error_handler(const std::string& logger_name, spdlog::log_err_handler err_handler) = 0;

protected:
    base_async_log_helper(
        size_t queue_size,
        const async_overflow_policy overflow_policy = async_overflow_policy::block_retry,
        const std::chrono::milliseconds& flush_interval_ms = std::chrono::milliseconds::zero());

    // queue of messages to log
    q_type _q;

    // overflow policy
    const async_overflow_policy _overflow_policy;

    // auto periodic sink flush parameter
    const std::chrono::milliseconds _flush_interval_ms;

    void push_msg(async_msg&& new_msg);

    // sleep,yield or return immediately using the time passed since last message as a hint
    static void sleep_or_yield(const spdlog::log_clock::time_point& now, const log_clock::time_point& last_op_time);

    // wait until the queue is empty
    void wait_empty_q();

    // worker thread main loop
    virtual void worker_loop() = 0;

    // pop next message from the queue and process it. will set the last_pop to the pop time
    // return false if termination of the queue is required
    virtual bool process_next_msg(log_clock::time_point& last_pop, log_clock::time_point& last_flush) = 0;

    virtual void handle_flush_interval(log_clock::time_point& now, log_clock::time_point& last_flush) = 0;

};

class async_log_helper : public base_async_log_helper
{
public:
    async_log_helper(formatter_ptr formatter,
        const std::vector<sink_ptr>& sinks,
                size_t queue_size,
        const log_err_handler err_handler,
        const async_overflow_policy overflow_policy = async_overflow_policy::block_retry,
        const std::function<void()>& worker_warmup_cb = nullptr,
        const std::chrono::milliseconds& flush_interval_ms = std::chrono::milliseconds::zero(),
        const std::function<void()>& worker_teardown_cb = nullptr);

    ~async_log_helper() override;

    // TODO: all below should become virtual
    void set_formatter(const std::string& logger_name, formatter_ptr) override;

    void flush(const std::string& logger_name, bool wait_for_q) override;

    void set_error_handler(const std::string& logger_name, spdlog::log_err_handler err_handler) override;

private:
    // worker thread
    std::thread _worker_thread;

    formatter_ptr _formatter;
    std::vector<std::shared_ptr<sinks::sink>> _sinks;

    log_err_handler _err_handler;

    bool _flush_requested;

    bool _terminate_requested;

    // worker thread warmup callback - one can set thread priority, affinity, etc
    const std::function<void()> _worker_warmup_cb;

    // worker thread teardown callback
    const std::function<void()> _worker_teardown_cb;

    // TODO: all below should become virtual
    // worker thread main loop
    void worker_loop() override;

    // pop next message from the queue and process it. will set the last_pop to the pop time
    // return false if termination of the queue is required
    bool process_next_msg(log_clock::time_point& last_pop, log_clock::time_point& last_flush) override;

    void handle_flush_interval(log_clock::time_point& now, log_clock::time_point& last_flush) override;
};

class pooled_log_helper : public base_async_log_helper
{
public:
    static void create(size_t queue_size,
                       const log_err_handler err_handler,
                       const async_overflow_policy overflow_policy = async_overflow_policy::block_retry,
                       const std::function<void()>& worker_warmup_cb = nullptr,
                       const std::chrono::milliseconds& flush_interval_ms = std::chrono::milliseconds::zero(),
                       const std::function<void()>& worker_teardown_cb = nullptr);
    static std::shared_ptr<pooled_log_helper> get();

    ~pooled_log_helper() override;

    void register_logger(const std::string& logger_name,
                    formatter_ptr formatter,
                    const std::vector<sink_ptr>& sinks,
                    size_t queue_size,
                    const log_err_handler err_handler);

    // TODO: all below should become virtual
    void set_formatter(const std::string& logger_name, formatter_ptr) override;

    void flush(const std::string& logger_name, bool wait_for_q) override;

    void set_error_handler(const std::string& logger_name, spdlog::log_err_handler err_handler) override;

private:
    static std::shared_ptr<pooled_log_helper> _instance;

    // worker thread
    std::vector<std::thread> _worker_threads;

    struct logger_props {
        formatter_ptr formatter;
        std::vector<std::shared_ptr<sinks::sink>> sinks;
        log_err_handler err_handler;
    };

    std::unordered_map<std::string, logger_props> _loggers;
    std::mutex _loggers_mutex;

    std::deque<std::string> _flush_requested;
    std::mutex _flush_mutex;

    std::atomic<bool> _terminate_requested;

    pooled_log_helper(size_t queue_size,
                      const log_err_handler err_handler,
                      const async_overflow_policy overflow_policy = async_overflow_policy::block_retry,
                      const std::function<void()>& worker_warmup_cb = nullptr,
                      const std::chrono::milliseconds& flush_interval_ms = std::chrono::milliseconds::zero(),
                      const std::function<void()>& worker_teardown_cb = nullptr);

    // TODO: may be moved back to base
    // worker thread warmup callback - one can set thread priority, affinity, etc
    const std::function<void()> _worker_warmup_cb;

    // worker thread teardown callback
    const std::function<void()> _worker_teardown_cb;

    // TODO: all below should become virtual
    // worker thread main loop
    void worker_loop() override;

    // pop next message from the queue and process it. will set the last_pop to the pop time
    // return false if termination of the queue is required
    bool process_next_msg(log_clock::time_point& last_pop, log_clock::time_point& last_flush) override;

    void handle_flush_interval(log_clock::time_point& now, log_clock::time_point& last_flush) override;
};

}
}

///////////////////////////////////////////////////////////////////////////////
// async_sink class implementation
///////////////////////////////////////////////////////////////////////////////
inline spdlog::details::base_async_log_helper::base_async_log_helper(
    size_t queue_size,
    const async_overflow_policy overflow_policy,
    const std::chrono::milliseconds& flush_interval_ms):
    _q(queue_size),
    _overflow_policy(overflow_policy),
    _flush_interval_ms(flush_interval_ms)
{}


inline spdlog::details::async_log_helper::async_log_helper(
    formatter_ptr formatter,
    const std::vector<sink_ptr>& sinks,
    size_t queue_size,
    log_err_handler err_handler,
    const async_overflow_policy overflow_policy,
    const std::function<void()>& worker_warmup_cb,
    const std::chrono::milliseconds& flush_interval_ms,
    const std::function<void()>& worker_teardown_cb):
    base_async_log_helper(queue_size, overflow_policy, flush_interval_ms),
    _formatter(formatter),
    _sinks(sinks),
    _err_handler(err_handler),
    _flush_requested(false),
    _terminate_requested(false),
    _worker_warmup_cb(worker_warmup_cb),
    _worker_teardown_cb(worker_teardown_cb),
    _worker_thread(&async_log_helper::worker_loop, this)
{}

// Send to the worker thread termination message(level=off)
// and wait for it to finish gracefully
inline spdlog::details::async_log_helper::~async_log_helper()
{
    try
    {
        push_msg(async_msg("", async_msg_type::terminate));
        _worker_thread.join();
    }
    catch (...) // don't crash in destructor
    {
    }
}

inline spdlog::details::pooled_log_helper::~pooled_log_helper()
{
    try
    {
        push_msg(async_msg("", async_msg_type::terminate));
        for (auto& worker_thread: _worker_threads)
        {
            worker_thread.join();
        }
    }
    catch (...) // don't crash in destructor
    {
    }
}


//Try to push and block until succeeded (if the policy is not to discard when the queue is full)
inline void spdlog::details::base_async_log_helper::log(const details::log_msg& msg)
{
    push_msg(async_msg(msg));
}

inline void spdlog::details::base_async_log_helper::push_msg(details::base_async_log_helper::async_msg&& new_msg)
{
    if (!_q.enqueue(std::move(new_msg)) && _overflow_policy != async_overflow_policy::discard_log_msg)
    {
        auto last_op_time = details::os::now();
        auto now = last_op_time;
        do
        {
            now = details::os::now();
            sleep_or_yield(now, last_op_time);
        }
        while (!_q.enqueue(std::move(new_msg)));
    }
}

// optionally wait for the queue be empty and request flush from the sinks
inline void spdlog::details::async_log_helper::flush(const std::string& logger_name, bool wait_for_q)
{
    // TODO: may be moved back to base
    push_msg(async_msg(logger_name, async_msg_type::flush));
    if (wait_for_q)
        wait_empty_q(); //return only make after the above flush message was processed
}

inline void spdlog::details::pooled_log_helper::flush(const std::string& logger_name, bool wait_for_q)
{
    // TODO: may be moved back to base
    push_msg(async_msg(logger_name, async_msg_type::flush));
    if (wait_for_q)
        wait_empty_q(); //return only make after the above flush message was processed
}

inline void spdlog::details::async_log_helper::worker_loop()
{
    if (_worker_warmup_cb) _worker_warmup_cb();
    auto last_pop = details::os::now();
    auto last_flush = last_pop;
    auto active = true;
    while (active)
    {
        try
        {
            active = process_next_msg(last_pop, last_flush);
        }
        catch (const std::exception &ex)
        {
            _err_handler(ex.what());
        }
        catch (...)
        {
            _err_handler("Unknown exception");
        }
    }
    if (_worker_teardown_cb) _worker_teardown_cb();


}

inline void spdlog::details::pooled_log_helper::worker_loop()
{
    if (_worker_warmup_cb) _worker_warmup_cb();
    auto last_pop = details::os::now();
    auto last_flush = last_pop;
    auto active = true;
    while (active)
    {
        try
        {
            active = process_next_msg(last_pop, last_flush);
        }
        catch (const std::exception &ex)
        {
            for (auto& logger: _loggers) {
                logger.second.err_handler(ex.what());
            }
        }
        catch (...)
        {
            for (auto& logger: _loggers) {
                logger.second.err_handler("Unknown exception");
            }
        }
    }
    if (_worker_teardown_cb) _worker_teardown_cb();


}

// process next message in the queue
// return true if this thread should still be active (while no terminate msg was received)
inline bool spdlog::details::async_log_helper::process_next_msg(log_clock::time_point& last_pop, log_clock::time_point& last_flush)
{
    async_msg incoming_async_msg;

    if (_q.dequeue(incoming_async_msg))
    {
        last_pop = details::os::now();
        switch (incoming_async_msg.msg_type)
        {
        case async_msg_type::flush:
            _flush_requested = true;
            break;

        case async_msg_type::terminate:
            _flush_requested = true;
            _terminate_requested = true;
            break;

        default:
            log_msg incoming_log_msg;
            incoming_async_msg.fill_log_msg(incoming_log_msg);
            _formatter->format(incoming_log_msg);
            for (auto &s : _sinks)
            {
                if (s->should_log(incoming_log_msg.level))
                {
                    s->log(incoming_log_msg);
                }
            }
        }
        return true;
    }

    // Handle empty queue..
    // This is the only place where the queue can terminate or flush to avoid losing messages already in the queue
    else
    {
        auto now = details::os::now();
        handle_flush_interval(now, last_flush);
        sleep_or_yield(now, last_pop);
        return !_terminate_requested;
    }
}

inline bool spdlog::details::pooled_log_helper::process_next_msg(log_clock::time_point& last_pop, log_clock::time_point& last_flush)
{
    async_msg incoming_async_msg;

    if (_q.dequeue(incoming_async_msg))
    {
        last_pop = details::os::now();
        switch (incoming_async_msg.msg_type)
        {
            case async_msg_type::flush:
            {
                std::lock_guard<std::mutex> _lock(_flush_mutex);
                _flush_requested.push_back(incoming_async_msg.logger_name);
                break;
            }

            case async_msg_type::terminate:
                _terminate_requested = true;
                break;

            default:
                log_msg incoming_log_msg;
                incoming_async_msg.fill_log_msg(incoming_log_msg);

                auto& logger = _loggers[incoming_async_msg.logger_name];
                logger.formatter->format(incoming_log_msg);
                for (auto &s : logger.sinks)
                {
                    if (s->should_log(incoming_log_msg.level))
                    {
                        s->log(incoming_log_msg);
                    }
                }
        }
        return true;
    }

        // Handle empty queue..
        // This is the only place where the queue can terminate or flush to avoid losing messages already in the queue
    else
    {
        auto now = details::os::now();
        handle_flush_interval(now, last_flush);
        sleep_or_yield(now, last_pop);
        return !_terminate_requested;
    }
}

// flush all sinks if _flush_interval_ms has expired
inline void spdlog::details::async_log_helper::handle_flush_interval(log_clock::time_point& now, log_clock::time_point& last_flush)
{
    auto should_flush = _flush_requested || (_flush_interval_ms != std::chrono::milliseconds::zero() && now - last_flush >= _flush_interval_ms);
    if (should_flush)
    {
        for (auto &s : _sinks)
            s->flush();
        now = last_flush = details::os::now();
        _flush_requested = false;
    }
}

inline void spdlog::details::pooled_log_helper::handle_flush_interval(log_clock::time_point& now, log_clock::time_point& last_flush)
{
    if (_flush_mutex.try_lock()) {
        if (!_flush_requested.empty())
        {
            for (auto &name: _flush_requested)
                for (auto &s : _loggers[name].sinks)
                    s->flush();
            now = last_flush = details::os::now();
            _flush_requested.clear();
        } else if ((_flush_interval_ms != std::chrono::milliseconds::zero() &&
                    now - last_flush >= _flush_interval_ms) ||_terminate_requested)
        {
            for (auto &logger: _loggers)
                for (auto &s : logger.second.sinks)
                    s->flush();
            now = last_flush = details::os::now();
            _flush_requested.clear();
        }

        // TODO: how lock_guard works w/ try_lock strategy?
        _flush_mutex.unlock();
    }
}

inline void spdlog::details::async_log_helper::set_formatter(const std::string& logger_name, formatter_ptr msg_formatter)
{
    _formatter = msg_formatter;
}

inline void spdlog::details::pooled_log_helper::set_formatter(const std::string& logger_name, formatter_ptr msg_formatter)
{
    _loggers[logger_name].formatter = msg_formatter;
}

// spin, yield or sleep. use the time passed since last message as a hint
inline void spdlog::details::base_async_log_helper::sleep_or_yield(const spdlog::log_clock::time_point& now, const spdlog::log_clock::time_point& last_op_time)
{
    using namespace std::this_thread;
    using std::chrono::milliseconds;
    using std::chrono::microseconds;

    auto time_since_op = now - last_op_time;

    // spin upto 50 micros
    if (time_since_op <= microseconds(50))
        return;

    // yield upto 150 micros
    if (time_since_op <= microseconds(100))
        return std::this_thread::yield();

    // sleep for 20 ms upto 200 ms
    if (time_since_op <= milliseconds(200))
        return sleep_for(milliseconds(20));

    // sleep for 200 ms
    return sleep_for(milliseconds(200));
}

// wait for the queue to be empty
inline void spdlog::details::base_async_log_helper::wait_empty_q()
{
    auto last_op = details::os::now();
    while (_q.approx_size() > 0)
    {
        sleep_or_yield(details::os::now(), last_op);
    }
}

inline void spdlog::details::async_log_helper::set_error_handler(const std::string& logger_name, spdlog::log_err_handler err_handler)
{
    _err_handler = err_handler;
}

inline void spdlog::details::pooled_log_helper::set_error_handler(const std::string& logger_name, spdlog::log_err_handler err_handler)
{
    _loggers[logger_name].err_handler = err_handler;
}
