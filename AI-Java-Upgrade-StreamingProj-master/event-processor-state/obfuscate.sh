#!/bin/bash                                                                                                                                                            
mkdir fat                                                                                                                                                              
mkdir lean                                                                                                                                                             
mv target/event-processor-state-1.0.jar fat/                                                                                                                           
mv target/original-event-processor-state-1.0.jar lean/                                                                                                                 
java -Xms4096m -Xmx4096m -jar obfuscator-core.jar --jarIn lean/original-event-processor-state-1.0.jar --jarOut lean/original-event-processor-state-1.0-obf.jar --config obfuscator-config.json                                                                                                                                                 
rm -rf lean/original-event-processor-state-1.0.jar                                                                                                                     
unzip fat/event-processor-state-1.0.jar -d fat/ && rm -f fat/event-processor-state-1.0.jar                                                                             
unzip lean/original-event-processor-state-1.0-obf.jar -d lean/ && rm -f lean/original-event-processor-state-1.0-obf.jar                                                
rm -rf fat/com/zif                                                                                                                                                     
cp -R lean/com/zif fat/com/                                                                                                                                            
(cd fat;zip -r ../docker/event-processor-state-1.0.jar .)                                                                                                              
rm -rf fat                                                                                                                                                             
rm -rf lean
