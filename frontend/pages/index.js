export default function Home() {
  const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
  return (
    <main style={{padding:20,fontFamily:'sans-serif'}}>
      <h1>ClearCare Compliance – Admin</h1>
      <p>API base: {apiUrl}</p>
      <ul>
        <li><a href={apiUrl + '/docs'} target='_blank' rel='noreferrer'>Open API docs</a></li>
      </ul>
    </main>
  );
}
