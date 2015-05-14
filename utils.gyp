{
  'variables': {
    'utils_sources' :  [
                          'src/utils/constants.hh',          'src/utils/active_queue.hh',
                          'src/utils/flex_alloc.hh',         'src/utils/mempool.hh',
                          'src/utils/barrier.cc',            'src/utils/barrier.hh',
                          'src/utils/relative_time.cc',      'src/utils/relative_time.hh',
                          'src/utils/exception.hh',
                          'src/utils/net.cc',                'src/utils/net.hh',
                          'src/utils/hex_util.cc',           'src/utils/hex_util.hh',
                          'src/utils/async_worker.cc',       'src/utils/async_worker.hh',
                          'src/utils/table_collector.hh',
                          'src/utils/timer_service.cc',      'src/utils/timer_service.hh',
                          'src/utils/utf8.cc',               'src/utils/utf8.hh',
                        ],
  },
  'target_defaults': {
    'default_configuration': 'Debug',
    'configurations': {
      'Debug': {
        'defines':  [ 'DEBUG', '_DEBUG', ],
        'cflags':   [ '-O0', '-g3', ],
        'ldflags':  [ '-g3', ],
        'xcode_settings': {
          'OTHER_CFLAGS':  [ '-O0', '-g3', ],
          'OTHER_LDFLAGS': [ '-g3', ],
        },
      },
      'Release': {
        'defines':  [ 'NDEBUG', 'RELEASE', ],
        'cflags':   [ '-O3', ],
        'xcode_settings': {
        },
      },
    },
    'include_dirs': [
                      './src/',
                      '/usr/local/include/',
                      '/usr/include/',
                    ],
    'cflags': [
                '-Wall',
                '-fPIC',
                '-std=c++11',
              ],
    'defines':  [
                  'PIC',
                  'STD_CXX_11',
                  '_THREAD_SAFE',
                ],
  },
  'conditions': [
    ['OS=="mac"', {
     'defines':            [ 'UTILS_MAC_BUILD', ],
     'xcode_settings':  {
       'GCC_ENABLE_CPP_EXCEPTIONS':    'YES',
       'OTHER_CFLAGS':               [ '-std=c++11', ],
     },
     },
    ],
    ['OS=="linux"', {
     'defines':            [ 'UTILS_LINUX_BUILD', ],
     'link_settings': {
       'ldflags':   [ '-Wl,--no-as-needed', ],
       'libraries': [ '-lrt', ],
      },
     },
    ],
  ],
  'targets' : [
    {
      'conditions': [
        ['OS=="mac"', {
          'variables':  { 'utils_root':  '<!(pwd)/../', },
          'xcode_settings':  {
            'GCC_ENABLE_CPP_EXCEPTIONS':    'YES',
            'OTHER_CFLAGS':               [ '-std=c++11', ],
          },
          'direct_dependent_settings': {
            'include_dirs': [ '<(utils_root)/', ],
          },},],
        ['OS=="linux"', {
          'direct_dependent_settings': {
            'include_dirs':       [ '.', ],
          },},],
      ],
      'target_name':                   'utils',
      'type':                          'static_library',
      'defines':                     [ 'USING_UTILS_LIB',  ],
      'sources':                     [ '<@(utils_sources)', ],
    },
    {
      'target_name':       'utils_test',
      'type':              'executable',
      'dependencies':  [ 'utils', 'deps_/gtest/gyp/gtest.gyp:gtest_lib', ],
      'include_dirs':  [ './deps_/gtest/include/', ],
      'sources':       [ 'test/utils_test.cc', ],
    },
  ],
}