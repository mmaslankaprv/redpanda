

module_dir = os.path.join(vconfig.src_dir, 'infra', 'modules', 'cluster')
tf_bin = os.path.join(vconfig.infra_bin_dir, 'terraform')
cmd = f'cd {module_dir} && {tf_bin} output -json ip'
out = shell.run_oneline(cmd, env=vconfig.environ)
ips = json.loads(out)

os.makedirs(f'{vconfig.build_root}/ansible/', exist_ok=True)
invfile = f'{vconfig.build_root}/ansible/hosts.ini'
with open(invfile, 'w') as f:
    for ip in ips:
        f.write(f'{ip} ansible_user=root ansible_become=True\n')

ansbin = f'{vconfig.build_root}/venv/v/bin/ansible-playbook'
cmd = f'{ansbin} --private-key {ssh_key} -i {invfile} -v {playbook}'
