import paramiko

ssh = paramiko.SSHClient()
key = paramiko.AutoAddPolicy()
ssh.set_missing_host_key_policy(key)
ssh.connect(hostname="17.149.226.170", username="username", password="sp", allow_agent=False, look_for_keys=False)
cmdln = "pwd"
stdin, stdout, stderr = ssh.exec_command(cmdln)
for line in stdout:
    print line
