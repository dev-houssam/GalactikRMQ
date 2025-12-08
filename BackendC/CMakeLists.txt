cmake_minimum_required(VERSION 3.28.3) 
project(libGalactikRMQC)

# Crée des variables avec les fichiers à compiler
set(SRCS
    BackLibGalactikRMQ.c
    )

set(HEADERS
    BackLibGalactikRMQ.h
    )

add_library(GalactikRMQC ${SRCS} ${HEADERS})

install(TARGETS GalactikRMQC LIBRARY DESTINATION lib
                      	     ARCHIVE DESTINATION lib)
