


#include <functional>

class OnBlockExit {

  std::function<void()> func_;

public:
  ~OnBlockExit() {
    func_();
  }

  OnBlockExit(std::function<void()> func) : func_(func) {}

};

