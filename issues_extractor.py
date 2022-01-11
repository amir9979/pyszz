import jira
import git
import time
import re
import json
from datetime import datetime


class Issue(object):
    def __init__(self, issue_id, type, priority, resolution, url, creation_time):
        self.issue_id = issue_id
        self.type = type
        self.priority = priority
        self.resolution = resolution
        self.url = url
        self.creation_time = creation_time

    def to_saveable_dict(self):
        return {'issue_id': self.issue_id, 'type': self.type, 'priority': self.priority, 'resolution': self.resolution,
                'url': self.url, 'creation_time': self.creation_time}

    def to_features_dict(self):
        return {'issue_id': self.issue_id, 'type': self.type, 'priority': self.priority, 'resolution': self.resolution}



class JiraIssue(Issue):
    def __init__(self, issue, base_url):
        super().__init__(issue.key.strip().split('-')[1], issue.fields.issuetype.name.lower(), JiraIssue.get_name_or_default(issue.fields.priority, 'minor'), JiraIssue.get_name_or_default(issue.fields.resolution, 'resolved'), base_url, datetime.strptime(issue.fields.created, "%Y-%m-%dT%H:%M:%S.%f%z"))
        self.fields = {}
        for k, v in dict(issue.fields.__dict__).items():
            if k.startswith("customfield_") or k.startswith("__"):
                continue
            if type(v) in [str, type(None), type(0), type(0.1)]:
                self.fields[k] = str(v)
            elif hasattr(v, 'name'):
                self.fields[k] = v.name.replace('\n', '').replace(';', '.,')
            elif type(v) in [list, tuple]:
                lst = []
                for item in v:
                    if type(item) in [str]:
                        lst.append(item)
                    elif hasattr(item, 'name'):
                        lst.append(item.name)
                self.fields[k] = "@@@".join(lst)
        for k in self.fields:
            self.fields[k] = ' '.join(self.fields[k].split())

    @staticmethod
    def get_name_or_default(val, default):
        if val:
            return val.name.lower()
        return default


def get_jira_issues(project_name, url="http://issues.apache.org/jira", bunch=100):
    jira_conn = jira.JIRA(url)
    all_issues=[]
    extracted_issues = 0
    sleep_time = 30
    while True:
        try:
            issues = jira_conn.search_issues("project={0}".format(project_name), maxResults=bunch, startAt=extracted_issues)
            all_issues.extend(issues)
            extracted_issues=extracted_issues+bunch
            if len(issues) < bunch:
                break
        except Exception as e:
            sleep_time = sleep_time * 2
            if sleep_time >= 480:
                raise e
            time.sleep(sleep_time)
    return list(map(lambda issue: JiraIssue(issue, url), all_issues))

def _clean_commit_message(commit_message):
    if "git-svn-id" in commit_message:
        return commit_message.split("git-svn-id")[0]
    return ' '.join(commit_message.split())


def fix_renamed_files(files):
    """
    fix the paths of renamed files.
    before : u'tika-core/src/test/resources/{org/apache/tika/fork => test-documents}/embedded_with_npe.xml'
    after:
    u'tika-core/src/test/resources/org/apache/tika/fork/embedded_with_npe.xml'
    u'tika-core/src/test/resources/test-documents/embedded_with_npe.xml'
    :param files: self._files
    :return: list of modified files in commit
    """
    new_files = []
    for file in files:
        if "=>" in file:
            if "{" and "}" in file:
                # file moved
                src, dst = file.split("{")[1].split("}")[0].split("=>")
                fix = lambda repl: re.sub(r"{[\.a-zA-Z_/\-0-9]* => [\.a-zA-Z_/\-0-9]*}", repl.strip(), file)
                new_files.extend(map(fix, [src, dst]))
            else:
                # full path changed
                new_files.extend(map(lambda x: x.strip(), file.split("=>")))
                pass
        else:
            new_files.append(file)
    return new_files


class CommittedFile(object):
    def __init__(self, sha, name, insertions, deletions):
        self.sha = sha
        self.name = fix_renamed_files([name])[0]
        if insertions.isnumeric():
            self.insertions = int(insertions)
            self.deletions = int(deletions)
        else:
            self.insertions = 0
            self.deletions = 0
        self.is_java = self.name.endswith(".java")
        self.is_test = 'test' in self.name

    def get_values(self):
        return {'commit_sha': self.sha, 'file_name': self.name, 'is_java': self.is_java, 'is_test': self.is_test, 'added_lines': self.insertions, 'deleted_lines': self.deletions}


def _get_commits_files(repo):
    data = repo.git.log('--numstat','--pretty=format:"sha: %H"').split("sha: ")
    comms = {}
    for d in data[1:]:
        d = d.replace('"', '').replace('\n\n', '\n').split('\n')
        commit_sha = d[0]
        comms[commit_sha] = []
        for x in d[1:-1]:
            insertions, deletions, name = x.split('\t')
            names = fix_renamed_files([name])
            comms[commit_sha].extend(list(map(lambda n: CommittedFile(commit_sha, n, insertions, deletions), names)))
    return dict(map(lambda x: (x, comms[x]), filter(lambda x: comms[x], comms)))


def _get_commits_files_status(repo):
    data = repo.git.log('--name-status','--pretty=format:"sha: %H"').split("sha: ")
    ans = []
    for d in data[1:]:
        d = d.replace('"', '').replace('\n\n', '\n').split('\n')
        commit_sha = d[0]
        #comms[commit_sha] = []
        for x in d[1:-1]:
            file_status = x.split('\t')
            modification_type = file_status[0][0]
            names = file_status[1:]
            ans.extend(list(map(lambda n: (commit_sha, n, modification_type), filter(lambda x: x.endswith('.java'), names))))
    return ans


class Commit(object):
    def __init__(self, bug_id, git_commit, issue=None, files=None, is_java_commit=True):
        self._commit_id = git_commit.hexsha
        self._repo_dir = git_commit.repo.working_dir
        self._issue_id = bug_id
        if files:
            self._files = files
        else:
            self._files = list(map(lambda f: CommittedFile(self._commit_id, f, '0', '0'), git_commit.stats.files.keys()))
        self._methods = list()
        self._commit_date = time.mktime(git_commit.committed_datetime.timetuple())
        self._commit_formatted_date = datetime.utcfromtimestamp(self._commit_date).strftime('%Y-%m-%d %H:%M:%S')
        self.issue = issue
        if issue:
            self.issue_type = self.issue.type
        else:
            self.issue_type = ''
        self.is_java_commit = is_java_commit
        self.is_all_tests = all(list(map(lambda x: not x.is_test, self._files)))

    @classmethod
    def init_commit_by_git_commit(cls, git_commit, bug_id='0', issue=None, files=None, is_java_commit=True):
        return Commit(bug_id, git_commit, issue, files=files, is_java_commit=is_java_commit)


def _commits_and_issues(repo, jira_issues):
    issues = dict(map(lambda x: (x.issue_id, x), jira_issues))
    issues_dates = sorted(list(map(lambda x: (x, issues[x].creation_time), issues)), key=lambda x: x[1], reverse=True)
    def replace(chars_to_replace, replacement, s):
        temp_s = s
        for c in chars_to_replace:
            temp_s = temp_s.replace(c, replacement)
        return temp_s

    def get_bug_num_from_comit_text(commit_text, issues_ids):
        text = replace("[]?#,:(){}'\"", "", commit_text.lower())
        text = replace("-_.=", " ", text)
        text = text.replace('bug', '').replace('fix', '')
        for word in text.split():
            if word.isdigit():
                if word in issues_ids:
                    return word
        return "0"

    commits = []
    java_commits = _get_commits_files(repo)
    for commit_sha in java_commits:
        git_commit = repo.commit(commit_sha)
        bug_id = "0"
        if all(list(map(lambda x: not x.is_java, java_commits[commit_sha]))):
            commit = Commit.init_commit_by_git_commit(git_commit, bug_id, None, java_commits[commit_sha], False)
            commits.append(commit)
            continue
        try:
            commit_text = _clean_commit_message(git_commit.message)
        except Exception as e:
            continue
        ind = 0
        for ind, (issue_id, date) in enumerate(issues_dates):
            date_ = date
            if date_.tzinfo:
                date_ = date_.replace(tzinfo=None)
            if git_commit.committed_datetime.replace(tzinfo=None) > date_:
                break
        issues_dates = issues_dates[ind:]
        bug_id = get_bug_num_from_comit_text(commit_text, set(map(lambda x: x[0], issues_dates)))
        commits.append(
            Commit.init_commit_by_git_commit(git_commit, bug_id, issues.get(bug_id), java_commits[commit_sha]))
    return commits


def extract_json(repo_path, jira_key, repo_full_name, out_json, out_non_tests_json):
    issues = get_jira_issues(jira_key)
    commits = _commits_and_issues(git.Repo(repo_path), issues)
    # save_to_json(commits, repo_full_name, out_json)
    # save_to_json(list(filter(lambda x: not x.is_all_tests, commits)), repo_full_name, out_non_tests_json)
    to_many_files = list(filter(lambda x: len(x._files) < 6, commits))
    save_to_json(to_many_files, repo_full_name, out_json)
    save_to_json(list(filter(lambda x: not x.is_all_tests, to_many_files)), repo_full_name, out_non_tests_json)


def save_to_json(commits, repo_full_name, out_json):
    issued_ = list(filter(lambda c: c.issue is not None, commits))
    buggy = list(filter(lambda c: c.issue.type.lower() == 'bug', issued_))
    bugs_json = list(map(lambda c: {"repo_name": repo_full_name, 'fix_commit_hash': c._commit_id,
                                    "earliest_issue_date": c.issue.creation_time.strftime("%Y-%m-%dT%H:%M:%SZ")},
                         buggy))
    with open(out_json, 'w') as out:
        json.dump(bugs_json, out)


if __name__ == "__main__":
    import git
    from functools import reduce
    import pandas as pd
    d = _get_commits_files_status(git.Repo(r"c:\temp\camel2"))
    changes = []
    for c in _get_commits_files(git.Repo(r"c:\temp\camel2")).values():
        for f in c:
            changes.append(f.get_values())
    d2 = pd.DataFrame(changes)
    d2.to_csv(r"c:\temp\committed.csv", index=False)
    modifications = pd.DataFrame(d, columns=['commit_sha', 'file_name', 'modification_type'])
    modifications.to_csv(r"c:\temp\modifications.csv", index=False)
    modifications.merge(d2, on= ['commit_sha', 'file_name']).to_csv(r"c:\temp\all.csv", index=False)
    # extract_json(r"c:\temp\camel2", "CAMEL", 'apache/camel', 'camel_bugfixes.json', 'camel_non_tests_bugfixes.json')