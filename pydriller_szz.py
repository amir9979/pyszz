from pydriller import Repository, Git
import json
import pandas as pd


def pydriller_szz(git_path, bugsfixes_json, results_path):
    g = Git(git_path)
    with open(bugsfixes_json) as f:
        commits = json.loads(f.read())

    bic = {}
    for a in commits:
        bic[a['fix_commit_hash']] = {}
        c = next(Repository(git_path, single=a['fix_commit_hash']).traverse_commits())
        for f in c.modified_files:
            if '\\test\\' in f.new_path or not f.new_path.endswith('.java'):
                continue
            ans = g.get_commits_last_modified_lines(c, f)
            for f_name in ans:
                bic[a['fix_commit_hash']][f_name] = list(ans[f_name])

    with open(results_path + ".json", 'w') as out:
        json.dump(bic, out)

    as_csv = []
    for bugfix_commit in bic:
        for f_name in bic[bugfix_commit]:
            for bic_commit in bic[bugfix_commit][f_name]:
                as_csv.append([bugfix_commit, f_name, bic_commit])
    df = pd.DataFrame(as_csv, columns=['bugfix_commit', 'filename', 'bic'])
    df.to_csv(results_path+".csv", index=False)


if __name__ == "__main__":
    pydriller_szz(r"C:\Temp\checkouts\apache\camel", r"C:\Users\User\Documents\GitHub\pyszz\camel_bugfixes.json", r"c:\temp\pydriller_camle_bic")