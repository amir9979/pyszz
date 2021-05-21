import traceback
import tempfile
import json
import os
import logging as log
import subprocess
from typing import List, Set
from git import Commit
from ..szz.ma_szz import MASZZ
from ..options import Options


class RASZZ(MASZZ):
    """
        Refactoring Aware SZZ, improved version (RA-SZZ*). This version is based on Refactoring Miner 2.0.
        This is implemented at blame-level. It simply filters blame results by excluding lines that refer to refactoring
        operations detected by Refactoring Miner.

    """

    def __init__(self, repo_full_name: str, repo_url: str, repos_dir: str = None):
        super().__init__(repo_full_name, repo_url, repos_dir)
        self.refactorings = dict()
        
    def _extract_refactorings(self, commits):
        PATH_TO_REFMINER = os.path.join(Options.PYSZZ_HOME, 'tools/RefactoringMiner-2.0/bin/RefactoringMiner')
        
        for commit in commits:
            if not commit in self.refactorings:
                log.info(f'Running RefMiner on {commit}')
                command = [PATH_TO_REFMINER, "-c", self._repository_path, commit]
                try:
                    out = subprocess.check_output(command, stderr=subprocess.DEVNULL, timeout=300)
                    data = json.loads(out.decode('utf-8')) 
                    if 'commits' in data and len(data['commits']) > 0 and 'refactorings' in data['commits'][0]:
                        self.refactorings[commit] = data['commits'][0]['refactorings'] 
                    else:
                        log.info("Refactoring format corrupted for commit: {}".format(commit))
                        self.refactorings[commit] = []
                    #p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                except subprocess.CalledProcessError as e:
                    log.error(e)
                except subprocess.TimeoutExpired as e:
                    log.error("Command timed out: {}".format(e))
                    self.refactorings[commit] = []
    
    def get_impacted_files(self, fix_commit_hash: str,
                           file_ext_to_parse: List[str] = None,
                           only_deleted_lines: bool = True) -> List['ImpactedFile']:
        impacted_files = set(super().get_impacted_files(fix_commit_hash, file_ext_to_parse, only_deleted_lines))
        
        self._extract_refactorings([fix_commit_hash])
        
        for refactoring in self.refactorings[fix_commit_hash]:
            for location in refactoring['rightSideLocations']:
                file_path = location['filePath']
                from_line = location['startLine']
                to_line   = location['endLine']
                for f in impacted_files:
                    lines_to_remove = set()
                    for modified_line in f.modified_lines:
                        if file_path == f.file_path and modified_line >= from_line and modified_line <= to_line:
                            log.info(f'Ignoring {f.file_path} line {modified_line} (refactoring {refactoring["type"]})')
                            lines_to_remove.add(modified_line)
                    f.modified_lines = [line for line in f.modified_lines if not line in lines_to_remove]
        
        impacted_files = [f for f in impacted_files if len(f.modified_lines) > 0]
        return impacted_files
  
    def _blame(self, 
               rev: str,
               file_path: str,
               modified_lines: List[int],
               skip_comments: bool = False,
               ignore_revs_list: List[str] = None,
               ignore_revs_file_path: str = None,
               ignore_whitespaces: bool = False,
               detect_move_within_file: bool = False,
               detect_move_from_other_files: 'DetectLineMoved' = None
               ) -> Set['BlameData']:
        log.info("Running super-blame")
        candidate_blame_data = super()._blame(
            rev,
            file_path, 
            modified_lines, 
            skip_comments, 
            ignore_revs_list, 
            ignore_revs_file_path, 
            ignore_whitespaces, 
            detect_move_within_file, 
            detect_move_from_other_files
        )
        
        commits = set([blame.commit.hexsha for blame in candidate_blame_data])
        self._extract_refactorings(commits)
        
        to_reblame = dict()
        
        result_blame_data = set()
        for blame in candidate_blame_data:
            can_add = True
            for refactoring in self.refactorings[blame.commit.hexsha]:
                for location in refactoring['rightSideLocations']:
                    file_path = location['filePath']
                    from_line = location['startLine']
                    to_line   = location['endLine']
                    
                    if blame.file_path == file_path and blame.line_num >= from_line and blame.line_num <= to_line:
                        log.info(f'Ignoring {blame.file_path} line {blame.line_num} (refactoring {refactoring["type"]})')
                        if not (blame.commit.hexsha + "@" + blame.file_path) in to_reblame:
                            to_reblame[blame.commit.hexsha + "@" + blame.file_path] = ReblameCandidate(blame.commit.hexsha, blame.file_path, [blame.line_num])
                        else:
                            to_reblame[blame.commit.hexsha + "@" + blame.file_path].modified_lines.append(blame.line_num)
                        can_add = False
                    
            if can_add:
                result_blame_data.add(blame)
                
        for _, reblame_candidate in to_reblame.items():
            log.info(f'Re-blaming {reblame_candidate.file_path} @ {reblame_candidate.rev}, lines {reblame_candidate.modified_lines} because of refactoring')
            if reblame_candidate.rev in ignore_revs_list:
                continue
            new_ignore_revs_list = ignore_revs_list.copy()
            new_ignore_revs_list.append(reblame_candidate.rev)
            
            new_blame_results = self._blame(
                reblame_candidate.rev,
                reblame_candidate.file_path,
                reblame_candidate.modified_lines,
                skip_comments, 
                new_ignore_revs_list, 
                ignore_revs_file_path, 
                ignore_whitespaces, 
                detect_move_within_file, 
                detect_move_from_other_files
            )
            result_blame_data.update(new_blame_results)
        
        return result_blame_data

class ReblameCandidate:
    def __init__(self, rev, file_path, modified_lines):
        self.rev = rev
        self.file_path = file_path
        self.modified_lines = modified_lines
