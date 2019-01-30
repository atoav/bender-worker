# bender-worker

It is a multi-plattform client for the bender-renderfarm. It receives it's \
tasks via amqp/rabbitmq, requests blendfiles from flaskbender \
via http GET, renders the Tasks and stores the rendered Frames on disk.

###
You can configure it via `bender-worker --configure`. If you want to see what \
else is possible (besides just running it) check `bender-worker -h`

### Life of a task
1. Task is received via `work`-queue from rabbitmq, the delivery-tags get stored because the Tasks will only be ACK'd once they are done
2. The command stored in the Task gets constructed. This means the "abstract" paths stored insided the command get replaced with paths configured in the bender-worker (e.g. for reading blendfiles, or storing rendered frames)
3. Once constructed bender-worker generate a unique set of parent (Job) IDs, because it is likely that multiple tasks belong to the same job. For each unique ID a asynchronous http request to flaskbender is made, and the blend will be downloaded
4. Once the Task has a blendfile it gets dispatched asynchronously
5. Once the Task is done its delivery-tag gets ACK'd, the Task finished and the next Task will be selected
6. After a grace period the downloaded blendfile gets deleted if flaskbender says the job has actually been done
7. Inbetween all these steps the Task gets transmitted to bender-bookkeeper for housekeeping


License: MIT
