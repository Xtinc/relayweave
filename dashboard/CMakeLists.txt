# Python sources are architecture independent; building the package does not run Python.
install(FILES dashboard.py proxy_client.py proxy_protocol.py history_store.py ip_location.py service_runtime.py flow_map.py
    DESTINATION usr/lib/relayweave/dashboard COMPONENT dashboard)
install(FILES templates/dashboard.html
    DESTINATION usr/lib/relayweave/dashboard/templates COMPONENT dashboard)
install(PROGRAMS "${PROJECT_SOURCE_DIR}/packaging/dashboard/relayweave-dashboard"
    DESTINATION usr/bin COMPONENT dashboard)
install(FILES "${PROJECT_SOURCE_DIR}/packaging/dashboard/relayweave-dashboard.service"
    DESTINATION lib/systemd/system COMPONENT dashboard)
install(FILES dashboard.example.json
    DESTINATION etc/relayweave RENAME dashboard.json COMPONENT dashboard)
install(FILES README.md requirements.txt
    DESTINATION usr/share/doc/relayweave-dashboard COMPONENT dashboard)
install(PROGRAMS "${PROJECT_SOURCE_DIR}/scripts/provision_relayweave.sh"
    DESTINATION usr/sbin RENAME relayweave-provision-dashboard COMPONENT dashboard)
