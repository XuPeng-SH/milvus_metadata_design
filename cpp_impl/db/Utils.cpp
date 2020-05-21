#include "db/Utils.h"

namespace milvus {
namespace engine {
namespace utils {

int64_t
GetMicroSecTimeStamp() {
    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();

    return micros;
}

}
}
}
