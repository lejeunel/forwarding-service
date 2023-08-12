#!/usr/bin/bash

list=("ae2aa3d0-29a5-45b2-91ca-064348441aaf"
      "8c31d326-16cb-46e2-b738-3ddafa6e7b24"
      "0fd77dba-797b-4e83-a20f-a5612254ee15"
      "5d7b564d-5ca3-4072-b7f1-eed7533d04a4"
      "8cc2272c-e9f7-432a-99c9-c1e9bd96828f"
      "69f6cfc9-00d6-4e7a-9fef-14a242e66e26"
      "66e9bf55-0d87-45de-a48f-6c9aeec896c0"
      "4637c376-24e9-4b92-94ea-9aabe72486fa"
      "d5ba6568-2c4e-41ac-8374-f7b8810b8bdd"
      "70cc3417-9c00-4a95-9520-6e03cb1308a3"
      "1cbe25af-b159-4237-b482-d91357b99f91"
      "33cbe544-1a3e-4f3c-ac1a-70ba942ea55c"
      "3b704f00-6bcb-4784-927f-bd61f820dd42"
      "1b1154f4-99a2-46ae-a1a4-547e3afb702d"
      "a3b26124-0613-432b-8c79-7466466ca71c"
      "37c591f4-cde6-4655-b72b-0c6e682e1c5c"
     )
for i in "${list[@]}"; do
    echo "resuming job:" $i
    flask --app fsapp/dev resume-job --testing $i
done
