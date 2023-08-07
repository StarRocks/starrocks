#pragma once

#include <memory>
#include <stdexcept>
#include <string>

/**
 * @ingroup wrenbind17
 */
namespace wrenbind17 {
/**
     * @ingroup wrenbind17
     */
class Exception : public std::exception {
public:
    Exception() = default;

    explicit Exception(std::string msg) : msg(std::move(msg)) {}

    const char* what() const throw() override { return msg.c_str(); }

private:
    std::string msg;
};

/**
     * @ingroup wrenbind17
     */
class NotFound : public Exception {
public:
    NotFound() : Exception("Not found") {}
};

/**
     * @ingroup wrenbind17
     */
class BadCast : public Exception {
public:
    BadCast() : Exception("Bad cast") {}

    explicit BadCast(std::string msg) : Exception(std::move(msg)) {}
};

/**
     * @ingroup wrenbind17
     */
class RuntimeError : public Exception {
public:
    explicit RuntimeError(std::string msg) : Exception(std::move(msg)) {}
};

/**
     * @ingroup wrenbind17
     */
class CompileError : public Exception {
public:
    explicit CompileError(std::string msg) : Exception(std::move(msg)) {}
};
} // namespace wrenbind17
