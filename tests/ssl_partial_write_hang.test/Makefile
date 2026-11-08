export LD_PRELOAD=${TESTSBUILDDIR}/libpartialwrite.so
ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=3m
endif
$(shell mkdir -p ${DBDIR}/; echo "export LD_PRELOAD=${TESTSBUILDDIR}/libpartialwrite.so" >> ${DBDIR}/replicant_env_vars )
# The nodes need the preload library too
$(shell for n in ${CLUSTER}; do ssh -o StrictHostKeyChecking=no $$n "mkdir -p ${TESTSBUILDDIR}" && scp -o StrictHostKeyChecking=no ${TESTSBUILDDIR}/libpartialwrite.so $$n:${TESTSBUILDDIR}/; done)
