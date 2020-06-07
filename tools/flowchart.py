import re
import random
from pprint import pprint
from collections import OrderedDict
from fire import Fire
from graphviz import Digraph

LINE0 = '<BO(SS=131,N_CC=6,N_PC=8,N_SC=12,N_SF=[51],LSN=2)> | DONE | OK'
# LINE1= '<BO(SS=145,N_CC=7,N_PC=9,N_SC=13,N_SE=12,N_SF=[52],LSN=3)> | DONE | OK'
LINE1= '<NSO(SS=145,N_CC=7,N_PC=9,N_SC=13,N_SE=12,N_SF=[52],LSN=3)> | DONE | OK'
LINE2 = '<DCO(SS=176,,LSN=4)> | DONE | OK'
LINE3 = '<MO(SS=161,N_CC=8,N_PC=10,N_SC=14,N_SE=13,S_SE=[1:12],N_SF=[53],LSN=5)> | DONE | OK'
LINE4 = '<BO(SS=174,,N_SF=[54],LSN=6)> | DONE | Error code(70003): Specified snapshot holder not found'
LINE5 = '<MO(SS=161,N_CC=8,N_PC=10,N_SC=14,N_SE=13,S_SE=[1:12],N_SF=[53],LSN=8)> | DONE | OK'
LINE6 = '<MO(SS=161,N_CC=8,N_PC=10,N_SC=14,N_SE=13,S_SE=[1:12],N_SF=[53],LSN=9)> | DONE | OK'
LINE7 = '<BO(SS=174,,N_SF=[54],LSN=10)> | DONE | Error code(70003): Specified snapshot holder not found'
LINE8 = '<MO(SS=161,N_CC=8,N_PC=10,N_SC=14,N_SE=13,S_SE=[1:12],N_SF=[53],LSN=11)> | DONE | OK'
LINE9= '<NSO(SS=145,N_CC=7,N_PC=9,N_SC=13,N_SE=12,N_SF=[52],LSN=12)> | DONE | OK'
LINE10 = '<DCO(SS=176,,LSN=13)> | DONE | OK'
LINE11 = '<MO(SS=161,N_CC=8,N_PC=10,N_SC=14,N_SE=13,S_SE=[1:12],N_SF=[53],LSN=14)> | DONE | OK'
# LINE = '<CCO(CID=0,CNAME="c1",N_CC=1,N_CID=1,N_CNAME="c1",N_PC=1,N_PID=1,N_PNAME="_default",LSN=2)> | DONE | OK'
LINES = [LINE0, LINE1, LINE2, LINE3, LINE4, LINE5, LINE6, LINE7, LINE8, LINE9, LINE10, LINE11]

BO_S_LINE = '<BO(LSN={})> | PENDING'
BO_P_LINE = '<BO(LSN={})> | DONE | OK'
BO_E_LINE = '<BO(LSN={})> | DONE | Error code(70003): xx'
MO_S_LINE = '<MO(LSN={})> | PENDING'
MO_P_LINE = '<MO(LSN={})> | DONE | OK'
MO_E_LINE = '<MO(LSN={})> | DONE | Error code(70004): xx'
DCO_S_LINE = '<DCO(LSN={})> | PENDING'
DCO_P_LINE = '<DCO(LSN={})> | DONE | OK'
DCO_E_LINE = '<DCO(LSN={})> | DONE | Error code(70005): xx'
NSO_S_LINE = '<NSO(LSN={})> | PENDING'
NSO_P_LINE = '<NSO(LSN={})> | DONE | OK'
NSO_E_LINE = '<NSO(LSN={})> | DONE | Error code(70006): xx'

TEMPLATES = [BO_S_LINE, BO_P_LINE, BO_E_LINE, MO_S_LINE, MO_P_LINE, MO_E_LINE,
        DCO_S_LINE, DCO_P_LINE, DCO_E_LINE, NSO_S_LINE, NSO_P_LINE, NSO_E_LINE]

def mock_lines(loop):
    lines = []
    pendings = []
    for i in range(1, loop+1):
        template_id = random.randint(0, len(TEMPLATES) - 1)
        line = TEMPLATES[template_id]
        lines.append(line.format(i))
        if line.endswith('PENDING'):
            pendings.append((template_id, i))
        fin_pending = random.randint(0, 9) > 5 and len(pendings) > 0
        if fin_pending:
            tid, lsn = pendings.pop(0)
            tid = random.randint(1,2) + tid
            line = TEMPLATES[tid]
            lines.append(line.format(lsn))

    return lines

class Parser:
    def parse_lines(self, lines):
        nodes = OrderedDict()
        for idx, line in enumerate(lines, 1):
            node = self.parse_line(line)
            if not node:
                continue
            nodes[idx] = node
        return nodes

    def parse_line(self, line):
        if not line:
            return {}
        lines = list(map(str.strip, line.split('|')))
        kvs = OrderedDict()
        kvs['node'] = self._parse_node(lines[0])
        kvs['lsn'] = kvs['node'].pop('lsn')
        kvs['state'] = lines[1]
        if len(lines) > 2:
            health = lines[2]
            kvs['health'] = self._parse_health(health)
        elif len(lines) == 2:
            kvs['health'] = {'status': 'UNKOWN'}

        return kvs

    def _parse_health(self, line):
        if not line:
            return {}
        r = OrderedDict()
        if line.startswith('OK'):
            r['status'] = 'OK'
        elif line.startswith('Error'):
            r['status'] = 'ERR'
            lines = list(map(str.strip, line.split(':')))
            assert len(lines) == 2, f'Invalid health info: {line}'
            m = re.match(r'Error code\((\w+)\)', lines[0])
            if not m:
                raise RuntimeError(f'Invalid health info: {line}')
            error_code = m.group(1)
            r['error_code'] = m.group(1)
            r['error_msg'] = lines[1]
        return r

    def _parse_node(self, line):
        if not line or not line.startswith('<') or not line.endswith('>'):
            raise RuntimeError(f'Invalid node info: {line}')
        r = OrderedDict()
        line = line[1:-1]
        m = re.match(r'(\w+)\((.*)\)', line)
        if not m:
            raise RuntimeError(f'Invalid node info: {line}')
        op = m.group(1)
        action_items = [s for s in map(str.strip, m.group(2).split(',')) if s]
        actions = OrderedDict()

        for item in action_items:
            tt = item.split('=')
            if tt[0] == 'LSN':
                r['lsn'] = int(tt[1])
                continue
            actions[tt[0]] = tt[1]

        r['name'] = op
        r['actions'] = actions
        return r

def create_start_node(g):
    start_id = '0'
    n = g.node(start_id, 'Start', shape='doublecircle')
    return start_id

def create_end_node(g):
    node_id = '-1'
    n = g.node(node_id, 'End', shape='doublecircle')
    return node_id

def create_cluster_node(g, node_id, node_info):
    node_name = str(node_info['lsn'])
    attrs = {}
    if node_info['health']['status'] == 'ERR':
        attrs['color'] = 'pink'
        error_code = node_info['health']['error_code']
        node_name = f'{node_name}:<{error_code}>'
    if node_info['state'] == 'PENDING':
        attrs['color'] = 'green'
        node_name = f'{node_name}:(P)'

    g.node(node_id, node_name, **attrs)
    return node_id

def create_node(g, node_id, node_info):
    node_name = f'{node_id}:{node_info["node"]["name"]}{node_info["lsn"]}'
    attrs = {}
    if node_info['health']['status'] == 'ERR':
        attrs['color'] = 'pink'
        error_code = node_info['health']['error_code']
        node_name = f'{node_name}:<{error_code}>'
    if node_info['state'] == 'PENDING':
        attrs['color'] = 'green'
        node_name = f'{node_name}:[P]'

    g.node(node_id, node_name, **attrs)
    return node_name

class Flow:
    def __init__(self, name, nodes, **kwargs):
        self.name = name
        self.nodes = nodes
        self.kwargs = kwargs

    def _build_subg(self, g, name, op_type, points, **kwargs):
        style = kwargs.get('style', 'filled')
        color = kwargs.get('color', 'lightgrey')
        node_style = kwargs.get('node_style', 'filled')
        node_color = kwargs.get('node_color', 'white')
        label = kwargs.get('label', None)
        label = label if label else name
        sub_points = []
        with g.subgraph(name=name) as c:
            c.attr(style=style, color=color)
            c.node_attr.update(style=node_style, color=node_color)
            for ts, node in self.nodes.items():
                node_id = str(ts)
                if node['node']['name'] != op_type:
                    continue
                create_cluster_node(c, node_id, node)
                points[ts] = node_id
                sub_points.append(node_id)
            if not sub_points:
                return

            edges = [[sub_points[0]]]
            for point in sub_points[1:]:
                edge = edges[-1]
                edge.append(point)
                edges.append([point])

            edges = [edge for edge in edges if len(edge) == 2]
            for edge in edges:
                lens = int(edge[1]) - int(edge[0])
                c.edge(*edge, style='invis', length=str(lens))
            c.attr(label=label)

    def draw_cluster(self):
        fmt = self.kwargs.get('format', 'svg')
        # 'dot', 'neato', 'twopi', 'circo', 'fdp', 'sfdp', 'patchwork', 'osage',
        g = Digraph(name=self.name, filename='cluster.gv', format=fmt, engine='dot')
        points = {}
        self._build_subg(g, 'cluster_0', 'BO', points, label='Build')
        self._build_subg(g, 'cluster_1', 'MO', points, color='lightblue', label='Merge')
        self._build_subg(g, 'cluster_2', 'NSO', points, color='lightblue2', label='NewSeg')
        self._build_subg(g, 'cluster_3', 'DCO', points, color='lightgrey', label='DropC')

        start_id = create_start_node(g)
        end_id = create_end_node(g)

        edges = [[start_id]]

        for lsn in sorted(points.keys()):
            edge = edges[-1]
            edge.append(points[lsn])
            edges.append([points[lsn]])

        edge = edges[-1]
        edge.append(end_id)

        for edge in edges:
            if len(edge) != 2:
                continue
            g.edge(*edge)

        g.view()

    def draw(self):
        fmt = self.kwargs.get('format', 'svg')
        # engine: fdp, sfdp, neato
        # 'dot', 'neato', 'twopi', 'circo', 'fdp', 'sfdp', 'patchwork', 'osage',
        dot = Digraph(name=self.name, filename='single.gv', format=fmt, engine='dot')
        dot.node_attr.update(style='filled', color='lightblue')
        start_id = create_start_node(dot)
        edges = [[start_id]]
        pendings = {}
        for ts, node in self.nodes.items():
            node_id = str(ts)
            create_node(dot, node_id, node)
            edge = edges[-1]
            edge.append(node_id)
            if node['state'] == 'PENDING':
                pendings[node['lsn']] = node_id
            else:
                start = pendings.pop(node['lsn'], None)
                if start and (int(node_id) - int(start)) > 1:
                    edges.append([start, node_id])
            edges.append([node_id])

        end_id = create_end_node(dot)
        edges[-1].append(end_id)

        for edge in edges:
            if len(edge) != 2:
                continue
            dot.edge(*edge)


        dot.view()

def draw(num=10, name='sample', cluster=False):
    p = Parser()
    nodes = p.parse_lines(mock_lines(num))
    # nodes = p.parse_lines(LINES)
    f = Flow(name, nodes)
    if cluster:
        f.draw_cluster()
    else:
        f.draw()

if __name__ == '__main__':
    Fire(draw)
