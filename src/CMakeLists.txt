set(TARGET tairzset_module)

set(SRCS
        tairzset.h
        tairzset.c
        redismodule.h )

add_library(${TARGET} SHARED ${SRCS} ${USRC})
set_target_properties(${TARGET} PROPERTIES SUFFIX ".so")
set_target_properties(${TARGET} PROPERTIES PREFIX "")