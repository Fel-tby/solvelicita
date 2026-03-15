import { useState, useEffect, useCallback, useMemo } from 'react'
import dynamic from 'next/dynamic'
import Head from 'next/head'
import { supabase } from '../lib/supabase'

const MapaCoropletico = dynamic(() => import('../components/MapaCoropletico'), {
  ssr: false,
  loading: () => (
    <div style={{ height: '100%', display: 'flex', alignItems: 'center',
      justifyContent: 'center', color: 'var(--text-lo)',
      fontFamily: 'JetBrains Mono, monospace', fontSize: '0.75rem', letterSpacing: '0.1em' }}>
      CARREGANDO MAPA...
    </div>
  )
})

const ORDEM_RISCO     = ['🟢 Risco Baixo', '🟡 Risco Médio', '🔴 Risco Alto', '⛔ Crítico', '⚫ Sem Dados']
const ORDEM_RISCO_IDX = Object.fromEntries(ORDEM_RISCO.map((r, i) => [r, i]))
const CORES_RISCO = {
  '🟢 Risco Baixo': 'var(--risk-baixo)',
  '🟡 Risco Médio': 'var(--risk-medio)',
  '🔴 Risco Alto':  'var(--risk-alto)',
  '⛔ Crítico':     'var(--risk-critico)',
  '⚫ Sem Dados':   'var(--risk-nd)',
}
const LABEL_RISCO = {
  '🟢 Risco Baixo': 'BAIXO',
  '🟡 Risco Médio': 'MÉDIO',
  '🔴 Risco Alto':  'ALTO',
  '⛔ Crítico':     'CRÍTICO',
  '⚫ Sem Dados':   'S/D',
}

const COLUNAS = [
  { key: '#',         label: '#',            field: null },
  { key: 'ente',      label: 'Município',    field: 'ente' },
  { key: 'score',     label: 'Score',        field: 'score' },
  { key: 'class',     label: 'Risco',        field: 'classificacao' },
  { key: 'pop',       label: 'Pop.',         field: 'populacao' },
  { key: 'eorcam',    label: 'Exec.Orç.%',   field: 'eorcam_raw' },
  { key: 'rproc',     label: 'RP Proc.',     field: 'n_anos_cronicos' },
  { key: 'siconfi',   label: 'SICONFI',      field: 'qsiconfi' },
  { key: 'cauc',      label: 'CAUC',         field: 'ccauc' },
  { key: 'lliq',      label: 'Lliq',         field: 'lliq_raw' },
  { key: 'autonomia', label: 'Autonomia',    field: 'autonomia_media' },
  { key: 'licit',     label: 'Licitações',   field: 'n_licitacoes' },
  { key: 'valor',     label: 'Val.Homolog.', field: 'valor_homologado_total' },
  { key: 'dispensa',  label: '% Dispensa',   field: 'pct_dispensa' },
  { key: 'alertas',   label: 'Alertas',      field: null },
]

function corPorScore(score) {
  if (score == null || isNaN(score)) return 'var(--risk-nd)'
  if (score >= 75) return 'var(--risk-baixo)'
  if (score >= 55) return 'var(--risk-medio)'
  if (score >= 35) return 'var(--risk-alto)'
  return 'var(--risk-critico)'
}

function fmtNum(v, d = 1) { return (v == null || isNaN(v)) ? '—' : Number(v).toFixed(d) }
function fmtPct(v, d = 1) { return (v == null || isNaN(v)) ? '—' : Number(v).toFixed(d) + '%' }
function fmtBRL(v) {
  if (v == null || isNaN(v)) return '—'
  if (v >= 1e9) return `R$ ${(v/1e9).toFixed(1)} bi`
  if (v >= 1e6) return `R$ ${(v/1e6).toFixed(1)} mi`
  return `R$ ${Number(v).toLocaleString('pt-BR')}`
}
function mediana(arr) {
  const s = arr.filter(v => v != null && !isNaN(v)).sort((a, b) => a - b)
  if (!s.length) return null
  const m = Math.floor(s.length / 2)
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2
}

function BadgeRisco({ classe }) {
  const cor = CORES_RISCO[classe] || 'var(--risk-nd)'
  return (
    <span style={{ display: 'inline-block', padding: '1px 6px', borderRadius: '2px',
      fontSize: '0.68rem', fontFamily: 'JetBrains Mono, monospace', fontWeight: 700,
      letterSpacing: '0.06em', color: cor, background: cor + '22', border: `1px solid ${cor}44` }}>
      {LABEL_RISCO[classe] || 'S/D'}
    </span>
  )
}

function AlertaBadge({ label, cor = '#f59e0b' }) {
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: '3px',
      padding: '1px 5px', borderRadius: '2px', fontSize: '0.62rem',
      fontFamily: 'JetBrains Mono, monospace', fontWeight: 700,
      color: cor, background: cor + '18', border: `1px solid ${cor}33` }}>
      ⚠ {label}
    </span>
  )
}

function KPI({ label, value, destaque }) {
  return (
    <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border)',
      borderTop: `3px solid ${destaque || 'var(--border)'}`,
      padding: '8px 10px', borderRadius: '3px', flex: 1, minWidth: 0 }}>
      <div style={{ fontSize: '0.55rem', color: 'var(--text-lo)', textTransform: 'uppercase',
        letterSpacing: '0.06em', fontFamily: 'JetBrains Mono, monospace', marginBottom: '3px',
        lineHeight: 1.3, wordBreak: 'break-word' }}>
        {label}
      </div>
      <div style={{ fontSize: '1.3rem', fontWeight: 700, color: 'var(--text-hi)',
        fontFamily: 'JetBrains Mono, monospace', lineHeight: 1.1 }}>
        {value}
      </div>
    </div>
  )
}

function PainelTitulo({ children }) {
  return (
    <div style={{ fontSize: '0.6rem', textTransform: 'uppercase', letterSpacing: '0.1em',
      color: 'var(--text-lo)', fontFamily: 'JetBrains Mono, monospace',
      paddingBottom: '7px', marginBottom: '9px', borderBottom: '1px solid var(--border-dim)' }}>
      {children}
    </div>
  )
}

function Painel({ children, style }) {
  return (
    <div style={{ background: 'var(--bg-panel)', border: '1px solid var(--border)',
      borderRadius: '3px', padding: '12px 14px', ...style }}>
      {children}
    </div>
  )
}

export default function Dashboard() {
  const [municipios, setMunicipios]         = useState([])
  const [geoData, setGeoData]               = useState(null)
  const [loading, setLoading]               = useState(true)
  const [erro, setErro]                     = useState(null)
  const [filtroRisco, setFiltroRisco]       = useState(new Set(ORDEM_RISCO))
  const [scoreRange, setScoreRange]         = useState([0, 100])
  const [busca, setBusca]                   = useState('')
  const [munSelecionado, setMunSelecionado] = useState(null)
  const [sortField, setSortField]           = useState('classificacao')
  const [sortAsc, setSortAsc]               = useState(true)
  const [tabelaExpandida, setTabelaExpandida] = useState(false)
  const [sidebarAberta, setSidebarAberta]   = useState(false)
  const [isMobile, setIsMobile]             = useState(false)
  const PREVIEW_LINHAS = 10

  useEffect(() => {
    function checar() { setIsMobile(window.innerWidth < 768) }
    checar()
    window.addEventListener('resize', checar)
    return () => window.removeEventListener('resize', checar)
  }, [])

  useEffect(() => {
    async function carregar() {
      try {
        const { data, error } = await supabase.from('municipios').select('*')
        if (error) throw error
        const geoResp = await fetch('/pb_geo.geojson')
        if (!geoResp.ok) throw new Error('GeoJSON não encontrado em /public/pb_geo.geojson')
        const geo = await geoResp.json()
        setMunicipios(data)
        setGeoData(geo)
      } catch (e) {
        setErro(e.message)
      } finally {
        setLoading(false)
      }
    }
    carregar()
  }, [])

  const handleSort = useCallback((field) => {
    if (!field) return
    setSortField(prev => {
      if (prev === field) { setSortAsc(a => !a); return field }
      setSortAsc(true)
      return field
    })
  }, [])

  const municipiosFiltrados = useMemo(() => {
    const filtrados = municipios.filter(m => {
      const classe = m.classificacao || '⚫ Sem Dados'
      if (!filtroRisco.has(classe)) return false
      if (m.score != null && (m.score < scoreRange[0] || m.score > scoreRange[1])) return false
      if (busca.trim() && !m.ente?.toLowerCase().includes(busca.trim().toLowerCase())) return false
      return true
    })
    return [...filtrados].sort((a, b) => {
      if (sortField === 'classificacao') {
        const ia = ORDEM_RISCO_IDX[a.classificacao] ?? 99
        const ib = ORDEM_RISCO_IDX[b.classificacao] ?? 99
        if (ia !== ib) return sortAsc ? ia - ib : ib - ia
        return (b.score ?? -1) - (a.score ?? -1)
      }
      const va = a[sortField], vb = b[sortField]
      if (va == null && vb == null) return 0
      if (va == null) return 1
      if (vb == null) return -1
      if (typeof va === 'string') return sortAsc ? va.localeCompare(vb, 'pt-BR') : vb.localeCompare(va, 'pt-BR')
      return sortAsc ? va - vb : vb - va
    })
  }, [municipios, filtroRisco, scoreRange, busca, sortField, sortAsc])

  const ibgesFiltrados = useMemo(
    () => new Set(municipiosFiltrados.map(m => m.cod_ibge)),
    [municipiosFiltrados]
  )

  const comScore = municipios.filter(m => m.score != null)
  const mediaPB  = comScore.length ? comScore.reduce((a, m) => a + m.score, 0) / comScore.length : null
  const medPB    = mediana(comScore.map(m => m.score))
  const nBaixo   = municipios.filter(m => m.classificacao === '🟢 Risco Baixo').length
  const nAlto    = municipios.filter(m => ['🔴 Risco Alto', '⛔ Crítico'].includes(m.classificacao)).length
  const nAlertas = municipios.filter(m => m.alerta_dispensa || m.dado_suspeito || m.autonomia_critica).length

  const distribuicao = useMemo(() => {
    const c = {}; ORDEM_RISCO.forEach(r => c[r] = 0)
    municipios.forEach(m => { const r = m.classificacao || '⚫ Sem Dados'; if (r in c) c[r]++ })
    return c
  }, [municipios])

  const medianas = useMemo(() => ({
    'Exec. Orçamentária (%)':    fmtPct(mediana(municipios.map(m => m.eorcam_raw))),
    'RP Processados (contrib.)': fmtNum(mediana(municipios.map(m => m.contrib_rproc)), 2),
    'Conformidade SICONFI':      (() => { const v = mediana(municipios.map(m => m.qsiconfi)); return v != null ? fmtPct(v * 100, 0) : '—' })(),
    'Lliq / Rec. (RGF A05)':    fmtNum(mediana(municipios.map(m => m.lliq_raw)), 3),
    'Autonomia Tributária':      fmtNum(mediana(municipios.map(m => m.autonomia_media)), 3),
  }), [municipios])

  const alertas = useMemo(() => ({
    dispensa:  municipios.filter(m => m.alerta_dispensa).length,
    suspeito:  municipios.filter(m => m.dado_suspeito).length,
    autonomia: municipios.filter(m => m.autonomia_critica).length,
    cronicos:  municipios.filter(m => m.n_anos_cronicos >= 5).length,
    defasado:  municipios.filter(m => m.dado_defasado).length,
  }), [municipios])

  const toggleRisco = useCallback((classe) => {
    setFiltroRisco(prev => { const n = new Set(prev); n.has(classe) ? n.delete(classe) : n.add(classe); return n })
  }, [])

  if (loading) return (
    <div style={{ height: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center',
      color: 'var(--text-lo)', fontFamily: 'JetBrains Mono, monospace', fontSize: '0.75rem', letterSpacing: '0.12em' }}>
      CARREGANDO DADOS...
    </div>
  )

  if (erro) return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column',
      alignItems: 'center', justifyContent: 'center', gap: '8px',
      color: 'var(--risk-alto)', fontFamily: 'JetBrains Mono, monospace', fontSize: '0.8rem' }}>
      <span>ERRO AO CARREGAR DADOS</span>
      <span style={{ color: 'var(--text-lo)', fontSize: '0.7rem' }}>{erro}</span>
    </div>
  )

  // ── Conteúdo da sidebar (reutilizado em desktop e drawer mobile) ──────────
  const SidebarConteudo = () => (
    <>
      <div style={{ marginBottom: '18px', display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
        <div>
          <div style={{ fontSize: '0.88rem', fontWeight: 700, color: 'var(--text-hi)',
            fontFamily: 'JetBrains Mono, monospace', letterSpacing: '0.06em' }}>SOLVELICITA</div>
          <div style={{ fontSize: '0.58rem', color: 'var(--text-lo)',
            fontFamily: 'JetBrains Mono, monospace', marginTop: '2px' }}>SOLVÊNCIA MUNICIPAL · PB</div>
        </div>
        {isMobile && (
          <button onClick={() => setSidebarAberta(false)}
            style={{ background: 'none', border: 'none', color: 'var(--text-lo)',
              fontSize: '1.1rem', cursor: 'pointer', padding: '0', lineHeight: 1 }}>✕</button>
        )}
      </div>

      <div style={{ borderTop: '1px solid var(--border-dim)', paddingTop: '12px' }}>
        <div style={{ fontSize: '0.58rem', color: 'var(--text-lo)', textTransform: 'uppercase',
          letterSpacing: '0.1em', fontFamily: 'JetBrains Mono, monospace', marginBottom: '7px' }}>
          Classificação
        </div>
        {ORDEM_RISCO.map(classe => {
          const ativo = filtroRisco.has(classe)
          const cor = CORES_RISCO[classe]
          return (
            <div key={classe} onClick={() => toggleRisco(classe)} style={{
              display: 'flex', alignItems: 'center', gap: '7px',
              padding: '4px 6px', borderRadius: '2px', cursor: 'pointer', marginBottom: '2px',
              background: ativo ? cor + '14' : 'transparent',
              border: `1px solid ${ativo ? cor + '44' : 'transparent'}`,
              opacity: ativo ? 1 : 0.35, transition: 'all 0.15s',
            }}>
              <div style={{ width: '7px', height: '7px', borderRadius: '50%',
                background: ativo ? cor : 'var(--text-lo)', flexShrink: 0 }} />
              <span style={{ fontSize: '0.68rem', color: ativo ? 'var(--text-hi)' : 'var(--text-lo)',
                fontFamily: 'JetBrains Mono, monospace' }}>{LABEL_RISCO[classe]}</span>
              <span style={{ marginLeft: 'auto', fontSize: '0.62rem', color: 'var(--text-lo)',
                fontFamily: 'JetBrains Mono, monospace' }}>{distribuicao[classe] || 0}</span>
            </div>
          )
        })}
      </div>

      <div style={{ borderTop: '1px solid var(--border-dim)', paddingTop: '12px', marginTop: '10px' }}>
        <div style={{ fontSize: '0.58rem', color: 'var(--text-lo)', textTransform: 'uppercase',
          letterSpacing: '0.1em', fontFamily: 'JetBrains Mono, monospace', marginBottom: '5px' }}>Score</div>
        <div style={{ fontSize: '0.62rem', color: 'var(--text-mid)', marginBottom: '3px',
          fontFamily: 'JetBrains Mono, monospace' }}>{scoreRange[0]} — {scoreRange[1]}</div>
        <input type="range" min="0" max="100" value={scoreRange[0]}
          onChange={e => setScoreRange([+e.target.value, scoreRange[1]])}
          style={{ width: '100%', marginBottom: '3px', accentColor: 'var(--accent)' }} />
        <input type="range" min="0" max="100" value={scoreRange[1]}
          onChange={e => setScoreRange([scoreRange[0], +e.target.value])}
          style={{ width: '100%', accentColor: 'var(--accent)' }} />
      </div>

      <div style={{ borderTop: '1px solid var(--border-dim)', paddingTop: '12px', marginTop: '10px' }}>
        <div style={{ fontSize: '0.58rem', color: 'var(--text-lo)', textTransform: 'uppercase',
          letterSpacing: '0.1em', fontFamily: 'JetBrains Mono, monospace', marginBottom: '5px' }}>Município</div>
        <input type="text" placeholder="Ex: Campina Grande" value={busca}
          onChange={e => setBusca(e.target.value)}
          style={{ width: '100%', background: 'var(--bg-card)', border: '1px solid var(--border)',
            borderRadius: '2px', padding: '5px 7px', color: 'var(--text-hi)',
            fontSize: '0.7rem', fontFamily: 'JetBrains Mono, monospace', outline: 'none',
            boxSizing: 'border-box' }} />
      </div>

      <div style={{ marginTop: 'auto', paddingTop: '16px', borderTop: '1px solid var(--border-dim)' }}>
        <div style={{ fontSize: '0.56rem', color: 'var(--text-lo)',
          fontFamily: 'JetBrains Mono, monospace', lineHeight: 1.8 }}>
          SICONFI · CAUC/STN<br />FINBRA/DCA · PNCP<br />Período: 2020–2025<br />PNCP: 2023–2026
        </div>
      </div>
    </>
  )

  return (
    <>
      <Head>
        <title>SolveLicita — Solvência Municipal PB</title>
        <meta name="description" content="Score de capacidade de pagamento dos municípios da Paraíba" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
      </Head>

      {/* Overlay mobile */}
      {isMobile && sidebarAberta && (
        <div onClick={() => setSidebarAberta(false)} style={{
          position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.6)', zIndex: 99
        }} />
      )}

      {/* Sidebar drawer mobile */}
      {isMobile && (
        <aside style={{
          position: 'fixed', top: 0, left: 0, bottom: 0,
          width: '240px', background: '#080b11',
          borderRight: '1px solid var(--border-dim)',
          display: 'flex', flexDirection: 'column',
          padding: '16px 14px', overflowY: 'auto',
          transform: sidebarAberta ? 'translateX(0)' : 'translateX(-100%)',
          transition: 'transform 0.25s ease',
          zIndex: 100,
        }}>
          <SidebarConteudo />
        </aside>
      )}

      <div style={{ display: 'flex', height: '100vh', overflow: 'hidden' }}>

        {/* Sidebar desktop */}
        {!isMobile && (
          <aside style={{ width: '195px', minWidth: '195px', background: '#080b11',
            borderRight: '1px solid var(--border-dim)', display: 'flex',
            flexDirection: 'column', padding: '16px 14px', overflowY: 'auto' }}>
            <SidebarConteudo />
          </aside>
        )}

        {/* Main */}
        <main style={{ flex: 1, overflowY: 'auto', overflowX: 'hidden',
          padding: isMobile ? '12px' : '14px 16px',
          display: 'flex', flexDirection: 'column', gap: '10px', minWidth: 0 }}>

          {/* Header */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            {isMobile && (
              <button onClick={() => setSidebarAberta(true)} style={{
                background: 'var(--bg-card)', border: '1px solid var(--border)',
                borderRadius: '3px', color: 'var(--text-hi)', cursor: 'pointer',
                padding: '6px 10px', fontSize: '1rem', flexShrink: 0,
              }}>☰</button>
            )}
            <div style={{ borderLeft: '3px solid var(--accent)', paddingLeft: '11px', minWidth: 0 }}>
              <h1 style={{ fontSize: isMobile ? '0.85rem' : '1rem', fontWeight: 700,
                color: 'var(--text-hi)', letterSpacing: '0.04em', textTransform: 'uppercase',
                fontFamily: 'JetBrains Mono, monospace', margin: 0 }}>
                Capacidade de Pagamento — Paraíba
              </h1>
              <p style={{ fontSize: '0.62rem', color: 'var(--text-lo)',
                fontFamily: 'JetBrains Mono, monospace', marginTop: '2px', marginBottom: 0 }}>
                SCORE DE SOLVÊNCIA · {municipios.length} MUNICÍPIOS · REFERÊNCIA 2020–2025
              </p>
            </div>
          </div>

          {/* KPIs */}
          <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
            <KPI label="Score Médio PB" value={mediaPB ? mediaPB.toFixed(1) : '—'} />
            <KPI label="Score Mediano"  value={medPB   ? medPB.toFixed(1)   : '—'} />
            <KPI label="Risco Baixo"    value={nBaixo}   destaque="var(--risk-baixo)" />
            <KPI label="Alto + Crítico" value={nAlto}    destaque="var(--risk-alto)" />
            <KPI label="Alertas Ativos" value={nAlertas} destaque="#f59e0b" />
          </div>

          {/* Mapa + Painel direito */}
          <div style={{
            display: 'flex',
            flexDirection: isMobile ? 'column' : 'row',
            gap: '10px',
            height: isMobile ? 'auto' : '480px',
            minHeight: isMobile ? 'auto' : '480px',
          }}>
            {/* Mapa */}
            <div style={{
              flex: isMobile ? 'none' : '1 1 0',
              height: isMobile ? '320px' : '100%',
              minWidth: 0,
              background: 'var(--bg-panel)',
              border: '1px solid var(--border)',
              borderRadius: '3px', overflow: 'hidden',
              position: 'relative', zIndex: 0,
            }}>
              {geoData && (
                <MapaCoropletico
                  geoData={geoData}
                  municipios={municipios}
                  ibgesFiltrados={ibgesFiltrados}
                  corPorScore={corPorScore}
                  onSelect={setMunSelecionado}
                />
              )}
            </div>

            {/* Painel direito */}
            <div style={{
              width: isMobile ? '100%' : '270px',
              minWidth: isMobile ? 'unset' : '270px',
              display: 'flex', flexDirection: isMobile ? 'row' : 'column',
              flexWrap: isMobile ? 'wrap' : 'nowrap',
              gap: '8px',
              overflowY: isMobile ? 'visible' : 'auto',
            }}>
              <Painel style={{ flex: isMobile ? '1 1 100%' : 'none' }}>
                <PainelTitulo>Distribuição por Faixa de Risco</PainelTitulo>
                {ORDEM_RISCO.map(classe => {
                  const n   = distribuicao[classe] || 0
                  const pct = municipios.length ? n / municipios.length * 100 : 0
                  const cor = CORES_RISCO[classe]
                  return (
                    <div key={classe} style={{ display: 'flex', alignItems: 'center', gap: '7px',
                      padding: '3px 0', borderBottom: '1px solid var(--border-dim)',
                      fontFamily: 'JetBrains Mono, monospace', fontSize: '0.7rem' }}>
                      <span style={{ color: cor, minWidth: '48px' }}>{LABEL_RISCO[classe]}</span>
                      <div style={{ flex: 1, background: 'var(--bg-card)', borderRadius: '2px', height: '5px' }}>
                        <div style={{ width: `${pct}%`, height: '5px', background: cor, borderRadius: '2px', transition: 'width 0.4s' }} />
                      </div>
                      <span style={{ color: 'var(--text-mid)', minWidth: '22px', textAlign: 'right' }}>{n}</span>
                      <span style={{ color: 'var(--text-lo)', minWidth: '30px', textAlign: 'right' }}>{pct.toFixed(0)}%</span>
                    </div>
                  )
                })}
              </Painel>

              <Painel style={{ flex: isMobile ? '1 1 calc(50% - 4px)' : 'none' }}>
                <PainelTitulo>Indicadores — Mediana Estadual</PainelTitulo>
                {Object.entries(medianas).map(([label, val]) => (
                  <div key={label} style={{ display: 'flex', justifyContent: 'space-between',
                    padding: '4px 0', borderBottom: '1px solid var(--border-dim)',
                    fontFamily: 'JetBrains Mono, monospace', fontSize: '0.7rem' }}>
                    <span style={{ color: 'var(--text-lo)' }}>{label}</span>
                    <span style={{ color: 'var(--text-hi)', fontWeight: 600 }}>{val}</span>
                  </div>
                ))}
              </Painel>

              <Painel style={{ flex: isMobile ? '1 1 calc(50% - 4px)' : 'none' }}>
                <PainelTitulo>Alertas por Tipo</PainelTitulo>
                {[
                  { label: 'Dispensa > 30%',      val: alertas.dispensa,  cor: '#ef4444' },
                  { label: 'Dado suspeito',        val: alertas.suspeito,  cor: '#f59e0b' },
                  { label: 'Autonomia crítica',    val: alertas.autonomia, cor: '#f59e0b' },
                  { label: 'RP crônico (≥5 anos)', val: alertas.cronicos,  cor: '#ef4444' },
                  { label: 'Dado defasado',        val: alertas.defasado,  cor: '#64748b' },
                ].map(({ label, val, cor }) => (
                  <div key={label} style={{ display: 'flex', justifyContent: 'space-between',
                    alignItems: 'center', padding: '4px 0', borderBottom: '1px solid var(--border-dim)' }}>
                    <span style={{ fontSize: '0.68rem', color: 'var(--text-lo)',
                      fontFamily: 'JetBrains Mono, monospace' }}>{label}</span>
                    <span style={{ fontSize: '0.75rem', fontWeight: 700,
                      fontFamily: 'JetBrains Mono, monospace',
                      color: val > 0 ? cor : 'var(--text-lo)' }}>{val}</span>
                  </div>
                ))}
              </Painel>
            </div>
          </div>

          {/* Tabela */}
          <div style={{ flexShrink: 0, background: 'var(--bg-panel)', border: '1px solid var(--border)',
            borderRadius: '3px', overflow: 'auto' }}>
            <div style={{ padding: '7px 12px', borderBottom: '1px solid var(--border)',
              fontSize: '0.6rem', color: 'var(--text-lo)', textTransform: 'uppercase',
              letterSpacing: '0.1em', fontFamily: 'JetBrains Mono, monospace',
              display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span>
                Ranking — {tabelaExpandida ? municipiosFiltrados.length : Math.min(PREVIEW_LINHAS, municipiosFiltrados.length)} de {municipiosFiltrados.length} municípios
              </span>
              {!isMobile && (
                <span style={{ color: 'var(--text-lo)' }}>
                  clique nas colunas para ordenar
                  {busca && <span style={{ color: 'var(--accent)', marginLeft: '8px' }}>filtrado: "{busca}"</span>}
                </span>
              )}
            </div>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.73rem' }}>
              <thead>
                <tr style={{ background: 'var(--bg-card)' }}>
                  {COLUNAS.map(col => {
                    const ativo    = sortField === col.field
                    const clicavel = col.field !== null
                    return (
                      <th key={col.key} onClick={() => clicavel && handleSort(col.field)}
                        style={{ padding: '6px 9px', textAlign: 'left',
                          color: ativo ? 'var(--text-hi)' : 'var(--text-lo)',
                          fontWeight: 600, fontSize: '0.6rem', letterSpacing: '0.06em',
                          textTransform: 'uppercase', fontFamily: 'JetBrains Mono, monospace',
                          borderBottom: `1px solid ${ativo ? 'var(--accent)' : 'var(--border)'}`,
                          whiteSpace: 'nowrap', cursor: clicavel ? 'pointer' : 'default',
                          userSelect: 'none', transition: 'color 0.1s',
                        }}>
                        {col.label}
                        {ativo && <span style={{ marginLeft: '4px', opacity: 0.7 }}>{sortAsc ? '↑' : '↓'}</span>}
                      </th>
                    )
                  })}
                </tr>
              </thead>
              <tbody>
                {(tabelaExpandida ? municipiosFiltrados : municipiosFiltrados.slice(0, PREVIEW_LINHAS)).map((m, i) => {
                  const al = []
                  if (m.alerta_dispensa)      al.push({ label: 'DISPENSA',    cor: '#ef4444' })
                  if (m.dado_suspeito)        al.push({ label: 'SUSPEITO',    cor: '#f59e0b' })
                  if (m.autonomia_critica)    al.push({ label: 'AUT.CRÍTICA', cor: '#f59e0b' })
                  if (m.n_anos_cronicos >= 5) al.push({ label: 'RP CRÔNICO',  cor: '#ef4444' })
                  return (
                    <tr key={m.cod_ibge} onClick={() => setMunSelecionado(m)}
                      style={{ borderBottom: '1px solid var(--border-dim)', cursor: 'pointer',
                        background: munSelecionado?.cod_ibge === m.cod_ibge ? 'var(--accent)18' : 'transparent',
                        transition: 'background 0.1s' }}
                      onMouseEnter={e => { e.currentTarget.style.background = 'var(--bg-card)' }}
                      onMouseLeave={e => { e.currentTarget.style.background = munSelecionado?.cod_ibge === m.cod_ibge ? 'var(--accent)18' : 'transparent' }}>
                      <td style={{ padding: '5px 9px', color: 'var(--text-lo)', fontFamily: 'JetBrains Mono, monospace' }}>{i + 1}</td>
                      <td style={{ padding: '5px 9px', color: 'var(--text-hi)', fontWeight: 500, whiteSpace: 'nowrap' }}>{m.ente}</td>
                      <td style={{ padding: '5px 9px', fontFamily: 'JetBrains Mono, monospace', color: corPorScore(m.score), fontWeight: 700 }}>
                        {m.score != null ? m.score.toFixed(1) : '—'}
                      </td>
                      <td style={{ padding: '5px 9px' }}><BadgeRisco classe={m.classificacao} /></td>
                      <td style={{ padding: '5px 9px', color: 'var(--text-mid)', fontFamily: 'JetBrains Mono, monospace' }}>
                        {m.populacao?.toLocaleString('pt-BR') || '—'}
                      </td>
                      <td style={{ padding: '5px 9px', color: 'var(--text-mid)', fontFamily: 'JetBrains Mono, monospace' }}>
                        {fmtPct(m.eorcam_raw)}
                      </td>
                      <td style={{ padding: '5px 9px', fontFamily: 'JetBrains Mono, monospace',
                        color: m.n_anos_cronicos >= 5 ? '#ef4444' : 'var(--text-mid)' }}>
                        {m.n_anos_cronicos != null ? `${m.n_anos_cronicos}a` : '—'}
                      </td>
                      <td style={{ padding: '5px 9px', color: 'var(--text-mid)', fontFamily: 'JetBrains Mono, monospace' }}>
                        {m.qsiconfi != null ? fmtPct(m.qsiconfi * 100, 0) : '—'}
                      </td>
                      <td style={{ padding: '5px 9px', fontFamily: 'JetBrains Mono, monospace',
                        color: m.ccauc > 0 ? '#ef4444' : 'var(--text-mid)' }}>
                        {fmtNum(m.ccauc, 2)}
                      </td>
                      <td style={{ padding: '5px 9px', fontFamily: 'JetBrains Mono, monospace',
                        color: m.lliq_raw < 0 ? '#ef4444' : 'var(--text-mid)' }}>
                        {fmtNum(m.lliq_raw, 3)}
                      </td>
                      <td style={{ padding: '5px 9px', fontFamily: 'JetBrains Mono, monospace',
                        color: m.autonomia_media < 0.08 ? '#f59e0b' : 'var(--text-mid)' }}>
                        {fmtNum(m.autonomia_media, 3)}
                      </td>
                      <td style={{ padding: '5px 9px', color: 'var(--text-mid)', fontFamily: 'JetBrains Mono, monospace' }}>
                        {m.n_licitacoes?.toLocaleString('pt-BR') || '—'}
                      </td>
                      <td style={{ padding: '5px 9px', color: 'var(--text-mid)', fontFamily: 'JetBrains Mono, monospace' }}>
                        {fmtBRL(m.valor_homologado_total)}
                      </td>
                      <td style={{ padding: '5px 9px', fontFamily: 'JetBrains Mono, monospace',
                        color: m.pct_dispensa > 0.3 ? '#ef4444' : 'var(--text-mid)' }}>
                        {m.pct_dispensa != null ? fmtPct(m.pct_dispensa * 100) : '—'}
                      </td>
                      <td style={{ padding: '5px 9px' }}>
                        <div style={{ display: 'flex', gap: '3px', flexWrap: 'wrap' }}>
                          {al.map(a => <AlertaBadge key={a.label} label={a.label} cor={a.cor} />)}
                        </div>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>

            {municipiosFiltrados.length > PREVIEW_LINHAS && (
              <div onClick={() => setTabelaExpandida(prev => !prev)}
                style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '6px',
                  padding: '9px', cursor: 'pointer', borderTop: '1px solid var(--border-dim)',
                  color: 'var(--text-lo)', fontFamily: 'JetBrains Mono, monospace',
                  fontSize: '0.62rem', letterSpacing: '0.08em', textTransform: 'uppercase',
                  background: 'var(--bg-card)', transition: 'color 0.15s' }}
                onMouseEnter={e => e.currentTarget.style.color = 'var(--text-hi)'}
                onMouseLeave={e => e.currentTarget.style.color = 'var(--text-lo)'}>
                {tabelaExpandida
                  ? <>▲ <span>Colapsar — mostrar apenas top {PREVIEW_LINHAS}</span></>
                  : <>▼ <span>Ver todos os {municipiosFiltrados.length} municípios</span></>}
              </div>
            )}
          </div>

          {/* Rodapé */}
          <div style={{ fontSize: '0.6rem', color: 'var(--text-lo)', fontFamily: 'JetBrains Mono, monospace',
            borderTop: '1px solid var(--border-dim)', paddingTop: '8px', paddingBottom: '4px',
            display: 'flex', flexWrap: 'wrap', gap: '4px', justifyContent: 'space-between' }}>
            <span>SolveLicita · Dados públicos · SICONFI/Tesouro · CAUC/STN · FINBRA/DCA · PNCP</span>
            <span>
              <a href="https://github.com/Fel-tby/solvelicita" style={{ color: 'var(--text-lo)', textDecoration: 'none' }}>GitHub</a>
              {' · '}
              <a href="https://github.com/Fel-tby/solvelicita/blob/main/docs/METODOLOGIA.md"
                style={{ color: 'var(--text-lo)', textDecoration: 'none' }}>Metodologia</a>
            </span>
          </div>

        </main>
      </div>
    </>
  )
}