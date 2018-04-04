
## Verify AWS credentials
* Open aws console: https://alpfin.signin.aws.amazon.com/console
* Use credentials provided by coordinator
* Change password
* Logout

## Setup AWS access on laptop
* Install aws cli on laptop: https://docs.aws.amazon.com/cli/latest/userguide/cli-install-macos.html
* create new `credentials` file in ~/.aws directory (if does not exist)
* Add this content to credentials file
'[de_training]
aws_access_key_id = *****
aws_secret_access_key = *****'
* Replace ***** in above file with credentials provided by coordinator
* Clone https://github.com/ThoughtWorks-DPS/lab-cap-data-eng-trainee
* `cd lab-cap-data-eng-trainee`
* `./get-private-key.sh`
* `ssh-add dataeng-infra.pem`
* `ssh -o ProxyJump=ec2-user@bastion.training.twdps.io ec2-user@emr.training.twdps.io`
