//
//  FileDrain.h
//
//  Created by Alec Thilenius on 10/13/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once
#include <mutex>
#include <string>
#include <vector>
#include <sstream>

namespace Thilenius {
namespace MapReduce {
namespace DataDrain {

template<typename ReducePolicy> class FileDrain {
 public:
  typedef std::string InputType;

  FileDrain(InputType* outputPath) {
    m_outFile = new std::ofstream(*outputPath);
  }

  ~FileDrain() {
    m_outFile->close();
    SAFE_FREE(m_outFile);
  }

  // Thread Safe
  inline void AddData(typename ReducePolicy::KeyType key,
                      typename ReducePolicy::ValueType value) {
    std::lock_guard<std::mutex> guard(m_mutex);
    *m_outFile << value << ":" << key << std::endl;
  }

 private:
  std::ofstream* m_outFile;
  std::mutex m_mutex;
};

} // namespace DataDrain
} // namespace MapReduce
} // namespace Thilenius
