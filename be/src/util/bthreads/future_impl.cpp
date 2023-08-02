//
// Created by alex on 8/7/23.
//

#include "util/bthreads/future_impl.h"

#include <exception>
#include <system_error>

namespace starrocks::bthreads {
class future_error_category : public std::error_category {
public:
    const char* name() const noexcept override { return "bthread-future"; }

    std::error_condition default_error_condition(int ev) const noexcept override {
        switch (static_cast<future_errc>(ev)) {
        case future_errc::broken_promise:
            return std::error_condition{static_cast<int>(future_errc::broken_promise), future_category()};
        case future_errc::future_already_retrieved:
            return std::error_condition{static_cast<int>(future_errc::future_already_retrieved), future_category()};
        case future_errc::promise_already_satisfied:
            return std::error_condition{static_cast<int>(future_errc::promise_already_satisfied), future_category()};
        case future_errc::no_state:
            return std::error_condition{static_cast<int>(future_errc::no_state), future_category()};
        default:
            return std::error_condition{ev, *this};
        }
    }

    bool equivalent(std::error_code const& code, int condition) const noexcept override {
        return *this == code.category() && static_cast<int>(default_error_condition(code.value()).value()) == condition;
    }

    std::string message(int ev) const override {
        switch (static_cast<future_errc>(ev)) {
        case future_errc::broken_promise:
            return std::string{"Broken promise."};
        case future_errc::future_already_retrieved:
            return std::string{"Future already retrieved."};
        case future_errc::promise_already_satisfied:
            return std::string{"Promise already satisfied."};
        case future_errc::no_state:
            return std::string{"No associated state."};
        }
        return std::string{"Unknown error."};
    }
};

std::error_category const& future_category() noexcept {
    static future_error_category cat;
    return cat;
}

} // namespace starrocks::bthreads
