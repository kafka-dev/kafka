######################################################
# Project values
# Here at the top to make them easy to find

PROJECT	= kafkaconnect
VERSION_MAJOR = 1
VERSION_FULL = $(VERSION_MAJOR).0.1

#######################################################
# Core Values, used later in makefile.
# also defined values within the source code

MAKE_OPTS     =
OS            := $(shell uname -s)
MARCH         := $(shell uname -p)
DATESTRING    := $(shell date '+"%y-%m-%d"')
PWD           = `pwd`
CONFIG	      = max

#######################################################
# utility program names

CC = g++
CC_VERSION = $(shell $(CC) -dumpversion)
LD = $(CC)
AR = ar rcs
RM = rm -rf

THE_COMPILER = $(CC)

#######################################################
# build information

BUILD_TYPE = $(OS)-$(MARCH)-$(CC)-$(CC_VERSION)-$(CONFIG)

#######################################################
# Paths and Dirs

SRC_DIR   = src
SRC_PATH  = ./$(SRC_DIR)

BIN_DIR   = bin
BIN_PATH  = ./$(BIN_DIR)

BUILD_DIR  = $(BIN_PATH)/$(BUILD_TYPE)
OBJECT_DIR = $(BUILD_DIR)/obj
EXE_DIR    = $(BUILD_DIR)/bin
LIB_DIR    = $(BUILD_DIR)/lib
REPORTS_DIR = $(BIN_PATH)/reports

#######################################################
#                                                     #
# compiler and linker options                         #
#                                                     #
#######################################################

# Add -g if performing a debug build
DEBUG_FLAGS =
O_FLAGS =
MATH_FLAGS =

ifeq ($(OS),Darwin)
	L_FLAGS = -dynamiclib -dylib_install_name
else
	L_FLAGS = -shared -soname
endif

ifeq ($(CONFIG),debug)
	DEBUG_FLAGS = -g -ftemplate-depth-100
endif

ifeq ($(CONFIG),Os)
	O_FLAGS = -Os
endif

ifeq ($(CONFIG),O1)
	DEBUG_FLAGS = -g
	O_FLAGS = -O1
endif

ifeq ($(CONFIG),O2)
	O_FLAGS = -O2
endif

ifeq ($(CONFIG),O3)
	O_FLAGS = -O3
endif

ifeq ($(CONFIG),max)
	O_FLAGS = -O3 -funroll-loops -ffast-math -finline-functions -fomit-frame-pointer
endif

# base compiler flags
CFLAGS_BASE = \
	-DDATESTRING=$(DATESTRING) \
	-pipe \
	-Wall \
	-pthread \
	$(O_FLAGS) \
	$(DEBUG_FLAGS) \
	$(EXTRA_FLAGS)

# shared only flags
CFLAGS_SHARED =
ifneq ($(STATIC),true)
	CFLAGS_SHARED = -fPIC
endif

# flags for object files
CFLAGS = $(CFLAGS_BASE) $(CFLAGS_SHARED) -MD


# includes
INCLUDES = \
	-I./$(SRC_DIR) \
	

# Libs
LIBRARIES = \
        -L$(LIB_DIR)

#######################################################
#                                                     #
# Object Files                                        #
#                                                     #
#######################################################

#------------------------------------------------------
# library                                             |
#------------------------------------------------------

LIBRARY_OBJS = \
	$(OBJECT_DIR)/producer.o \

#------------------------------------------------------
# example                                             |
#------------------------------------------------------

PRODUCER_OBJS = \
	$(OBJECT_DIR)/example.o 

#------------------------------------------------------
# tests                                               |
#------------------------------------------------------

TEST_OBJS = \
	$(OBJECT_DIR)/tests/encoder_helper_tests.o \
	$(OBJECT_DIR)/tests/encoder_tests.o \
	$(OBJECT_DIR)/tests/producer_tests.o 


#######################################################
#                                                     #
# BUILD Targets                                       #
#                                                     #
#######################################################
default: sharedlibrary
.PHONY: all tests clean

#------------------------------------------------------
# library

STATIC_LIBRARY = lib$(PROJECT).a
SHARED_LIBRARY = lib$(PROJECT).so
SHARED_LIBRARY_MAJOR = $(SHARED_LIBRARY).$(VERSION_MAJOR)
SHARED_LIBRARY_FULL = $(SHARED_LIBRARY).$(VERSION_FULL)

ifeq ($(OS),Darwin)
	SHARED_LIB_PARAMS = -Wl,-dylib_install_name,$(SHARED_LIBRARY_FULL)
else
	SHARED_LIB_PARAMS = -Wl,-soname,$(SHARED_LIBRARY_FULL)
endif

staticlibrary: $(LIBRARY_OBJS)
	@echo linking $(STATIC_LIBRARY)
	@$(AR) $(STATIC_LIBRARY) $(LIBRARY_OBJS)

sharedlibrary: $(LIBRARY_OBJS)
	@echo linking $(SHARED_LIBRARY_FULL)
	@$(LD) -shared $(SHARED_LIB_PARAMS) \
		-o $(LIB_DIR)/$(SHARED_LIBRARY_FULL) \
		$(LIBRARIES) $(LIBRARY_OBJS)
	@ln -sf $(SHARED_LIBRARY_FULL) $(LIB_DIR)/$(SHARED_LIBRARY_MAJOR)
	@ln -sf $(SHARED_LIBRARY_FULL) $(LIB_DIR)/$(SHARED_LIBRARY)

LIBRARY_TYPE = sharedlibrary
LIBRARY_TARGET = $(LIB_DIR)/$(SHARED_LIBRARY_FULL)
ifeq ($(STATIC),true)
	LIBRARY_TYPE = staticlibrary
	LIBRARY_TARGET = $(LIB_DIR)/$(STATIC_LIBRARY)
endif

#------------------------------------------------------
# executable common

COMMON_EXTERNAL_LIBRARIES = \
	-lboost_system \
	-lboost_thread

#------------------------------------------------------
# producer example

PRODUCER_TARGET = $(EXE_DIR)/producer

producer: $(LIBRARY_TYPE) $(PRODUCER_OBJS)
	@echo linking $(PRODUCER_TARGET)
	@$(LD) -o $(PRODUCER_TARGET) \
		$(COMMON_EXTERNAL_LIBRARIES) \
		$(LIBRARIES) -lkafkaconnect \
		$(PRODUCER_OBJS) 

#------------------------------------------------------
# tests

TEST_LOG_FLAGS = 
ifeq ($(HUDSON_TESTS),true)
	TEST_LOG_FLAGS = --log_format=xml --log_level=all --log_sink=$(REPORTS_DIR)/kafkaconnect.xml
endif

TEST_TARGET = $(EXE_DIR)/test
TEST_RUN_COMMAND = $(TEST_TARGET) $(TEST_LOG_FLAGS)

test: $(LIBRARY_TYPE) $(TEST_OBJS)
	@echo linking $(TEST_TARGET)
	@$(LD) -o $(TEST_TARGET) \
		$(COMMON_EXTERNAL_LIBRARIES) \
		$(LIBRARIES) -lboost_unit_test_framework -lkafkaconnect \
		$(TEST_OBJS)
	@echo running $(TEST_RUN_COMMAND)
	LD_LIBRARY_PATH=.:$(LIB_DIR) $(TEST_RUN_COMMAND)
 
tests: test

all: $(LIBRARY_TYPE) producer tests

#------------------------------------------------------
#

install: $(LIBRARY_TYPE)
	@echo installing library to /usr/local/lib
	@cp $(LIBRARY_TARGET) /usr/local/lib/$(SHARED_LIBRARY_FULL)
	@ln -sf $(SHARED_LIBRARY_FULL) /usr/local/lib/$(SHARED_LIBRARY_MAJOR)
	@ln -sf $(SHARED_LIBRARY_FULL) /usr/local/lib/$(SHARED_LIBRARY)
	@ldconfig
	@echo installing headers to /usr/local/include/$(PROJECT)
	@if [ ! -d /usr/local/include/$(PROJECT) ]; then \
		mkdir -p /usr/local/include/$(PROJECT); \
	fi
	@cp $(SRC_DIR)/*.hpp /usr/local/include/$(PROJECT);

#------------------------------------------------------
# clean

clean:
	@echo cleaning 
	@rm -rfv $(LIBRARY_OBJS) $(KAFKA_EXAMPLE_OBJS) $(KAFKA_TEST_OBJS) \
		$(LIB_DIR)/$(STATIC_LIBRARY) $(LIB_DIR)/$(SHARED_LIBRARY) \
		$(LIB_DIR)/$(SHARED_LIBRARY_MAJOR) $(LIB_DIR)/$(SHARED_LIBRARY_FULL) \
		$(KAFKA_EXAMPLE_TARGET) $(KAFKA_TESTS_TARGET)

#######################################################
#                                                     #
# Build Rules / Macros                                #
#                                                     #
#######################################################

#######################################################
# macro to create object/binary paths                 #
#######################################################

define enforcepath
	@if [ ! -d $(OBJECT_DIR)/$(*D) ]; then \
		mkdir -p $(OBJECT_DIR)/$(*D); \
	 fi

	@if [ ! -d $(LIB_DIR) ]; then \
		mkdir -p $(LIB_DIR); \
	 fi

	@if [ ! -d $(EXE_DIR) ]; then \
		mkdir -p $(EXE_DIR); \
	 fi

	@if [ ! -d $(REPORTS_DIR) ]; then \
		mkdir -p $(REPORTS_DIR); \
	 fi
endef

#######################################################
# rule to make normal object files                    #
#######################################################

$(OBJECT_DIR)/%.o:	$(SRC_PATH)/%.cpp
	@$(enforcepath)
	@echo $(OBJECT_DIR)/$*.o,
	@$(THE_COMPILER) $(INCLUDES) $(CFLAGS) -c $(SRC_PATH)/$*.cpp -o $(OBJECT_DIR)/$*.o

