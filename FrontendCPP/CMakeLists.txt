# Nous voulons un cmake "récent" pour utiliser les dernières fonctionnalités
cmake_minimum_required(VERSION 3.28.3)

# Notre projet est étiqueté hello
project(GalactikRMQCPP)

# On inclut notre bibliothèque dans le processus de CMake
add_subdirectory(libGalactikRMQ)

# Crée des variables avec les fichiers à compiler
set(SRCS
	GalactikRMQCPP.c
    )
    
# Notre exécutable
add_executable(main ${SRCS})

# Et pour que l'exécutable fonctionne, il faut lui indiquer la bibliothèque dont il dépend
target_link_libraries(main GalactikRMQ)

# Indique que l'on installe l'exécutable dans un dossier bin
install (TARGETS main DESTINATION bin)
