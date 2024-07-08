import fnmatch, os, re
import numpy as np

def pattern_to_regex(pattern):
    pattern = fnmatch.translate(pattern)
    pattern = pattern.replace('.*', '(.*)')
    return re.compile(pattern)

def get_filenames(path='.', pattern='*', min_timestamp=0, extend=None, include_path=False):
    out = []
    regex = pattern_to_regex(pattern)
    if extend is not None:
        filenames = [e[0] for e in extend]
    with os.scandir(path) as iter:
        for entry in iter:
            name = entry.name
            match = regex.match(name)

            if match and entry.stat().st_ctime > min_timestamp:
                start, end = match.regs[1]
                try:
                    time = float(name[start:end])
                    if include_path:
                        name = os.path.join(path, name)
                    if extend is None or name not in filenames:
                        out.append((name, time))
                except:
                    pass

    out.sort(key=lambda x: x[1])
    if extend:
        extend.extend(out)
    return out


def get_creationtime(filename, path=os.getcwd()):
    return os.stat(os.path.join(path, filename)).st_ctime


def get_oldestfile(pattern, path=os.getcwd()):
    out = []
    regex = pattern_to_regex(pattern)
    min_timestamp = np.inf
    with os.scandir(path) as iter:
        for entry in iter:
            name = entry.name
            # print(name)
            match = regex.match(name)

            if match:
                creation_time = entry.stat().st_ctime
                if creation_time < min_timestamp:
                    start, end = match.regs[1]
                    try:
                        time = float(name[start:end])

                        out = (name, time)
                    except:
                        pass

    return out

def filename_without_extension(path):
    return os.path.splitext(os.path.basename(path))[0]

def ensure_directory_exists(path):
    dir=os.path.dirname(path)
    if not os.path.exists(dir):
        os.makedirs(dir)