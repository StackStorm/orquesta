# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import six


def merge_dicts(left, right, overwrite=True):
    if left is None:
        return right

    if right is None:
        return left

    for k, v in six.iteritems(right):
        if k not in left:
            left[k] = v
        else:
            left_v = left[k]

            if isinstance(left_v, dict) and isinstance(v, dict):
                merge_dicts(left_v, v, overwrite=overwrite)
            elif overwrite:
                left[k] = v

    return left


def get_dict_value(obj, path, raise_key_error=False):
    item = obj
    keys = path.split('.')
    traversed = ''

    for key in keys:
        if not isinstance(item, dict) and traversed != path:
            raise TypeError('Value of \'%s\' is not typeof dict.' % traversed)

        traversed += '.' + key if len(traversed) > 0 else key

        if key not in item and raise_key_error:
            raise KeyError('Key \'%s\' does not exist.' % traversed)

        item = item.get(key, None)

        if item is None:
            break

    return item
